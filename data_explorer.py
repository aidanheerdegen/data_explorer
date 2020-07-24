from collections import OrderedDict
import re

import cosima_cookbook as cc
import ipywidgets as widgets
from ipywidgets import HTML, Button, VBox, HBox, Label, Layout, Select
from ipywidgets import SelectMultiple, Tab, Text, Textarea, Checkbox
from ipywidgets import interact, interact_manual, AppLayout, Dropdown
import ipywidgets as wid

import pandas as pd

from cosima_cookbook.database import CFVariable, NCFile, NCExperiment, NCVar

from sqlalchemy import func

def return_value_or_empty(value):
    """Return value if not None, otherwise empty"""
    if value is None:
        return ''
    else:
        return value

class DatabaseExtension:

    session = None
    experiments = None
    keywords = None
    variables = None
    expt_variable_map = None
    restart_variables = None
    
    def __init__(self, session=None):
        if session is None:
            session = cc.database.create_session()
        self.session              = session
        self.experiments          = cc.querying.get_experiments(session, all=True)
        self.keywords             = sorted(cc.querying.get_keywords(session), key=str.casefold)
        self.expt_variable_map    = self.experiment_variable_map()
        self.variables            = self.unique_variable_list()

    def experiment_variable_map(self, experiments=None):
        """
        Make a pandas table with experiment as the index and columns
        of name, long_name and restart flag.

        Also make lists of unique name/long_name 
        """

        if experiments is None:
            experiments = self.experiments.experiment
        else:
            if isinstance(experiments, str):
                experiments = [experiments, ]

        allvars = pd.concat([self.get_variables(expt)
                     for expt in experiments], keys=experiments)

        # Create a new column to flag if variable is from a restart directory
        allvars['restart'] = allvars.ncfile.str.contains('restart')

        # Create a new column to flag if variable has units which match a number of criteria
        # that indicated it is a coordinate
        allvars = allvars.assign(coordinate=(allvars.units.str.contains('degrees', na=False) 
                                            | allvars.units.str.contains('since', na=False) 
                                            | allvars.units.str.match('^radians$', na=False)
                                            | allvars.units.str.startswith('days', na=False)))  # legit units: %/day, day of year

        return allvars[['name', 'long_name', 'restart', 'coordinate']]

    def unique_variable_list(self):
        """
        Extract a list of all variable name/long_name pairs from the experiment
        keyword map
        """
        return self.expt_variable_map.reset_index(drop=True).drop_duplicates()
        
    def get_restart_variables(self):
        """
        Return a table of all variables (name/long_name) from a restart directory
        """
        return self.variables[self.variables.restart]

    def keyword_filter(self, keywords):
        """
        Return a list of experiments matching *all* of the supplied keywords
        """
        try:
            return cc.querying.get_experiments(self.session, keywords=keywords).experiment
        except AttributeError:
            return []

    def variable_filter(self, variables):
        """
        Return a set of experiments that contain all the defined variables
        """
        expts = []
        for v in variables:
            expts.append(
                set(self.expt_variable_map[self.expt_variable_map.name == v].reset_index()['experiment'])
            )
        return set.intersection(*expts)
    
    def get_experiment(self, experiment):
        return self.experiments[self.experiments['experiment'] == experiment]

    # Return more metadata than get_variables from cosima-cookbook
    def get_variables(self, experiment, frequency=None):
        """
        Returns a DataFrame of variables for a given experiment and optionally
        a given diagnostic frequency.
        """

        q = (self.session
            .query(CFVariable.name,
                    CFVariable.long_name,
                    CFVariable.standard_name,
                    CFVariable.units,
                    NCFile.frequency,
                    NCFile.ncfile,
                    func.count(NCFile.ncfile).label('# ncfiles'),
                    func.min(NCFile.time_start).label('time_start'),
                    func.max(NCFile.time_end).label('time_end'))
            .join(NCFile.experiment)
            .join(NCFile.ncvars)
            .join(NCVar.variable)
            .filter(NCExperiment.experiment == experiment)
            .order_by(NCFile.frequency,
                    CFVariable.name,
                    NCFile.time_start,
                    NCFile.ncfile)
            .group_by(CFVariable.name, NCFile.frequency))

        if frequency is not None:
            q = q.filter(NCFile.frequency == frequency)

        return pd.DataFrame(q)

class VariableSelector(VBox):
    """
    Combo widget based on a Select box with a search panel above to live
    filter variables to Select. When a variable is selected the long name
    attribute is displayed under the select box. There are also two 
    checkboxes which hide coordinates and restart variables.

    Note that a dict is used to populate the Select widget, so the visible
    value is the variable name and is accessed via the label attribute, 
    and the long name via the value attribute. 
    """

    variables = None
    widgets = {}

    def __init__(self, variables, **kwargs):
        """
        variables is a pandas dataframe. kwargs are passed through to child
        widgets which, theoretically, allows for layout information to be
        specified
        """

        # Add a new column to keep track of visibility in widget
        self.variables = variables.assign(visible=True)

        # Variable search
        self.widgets['search'] = Text(
            placeholder='Search: start typing', 
            layout={'width': 'auto'},
        )
        # Variable selection box
        self.widgets['selector'] = Select(
            options=sorted(self.variables.name, key=str.casefold),
            rows=10,
            layout={'width': 'auto'},
        )
        # Variable info
        self.widgets['info'] = HTML(
            # description='Search', 
            layout={'width': '80%'},
        )
        # Variable filtering elements
        self.widgets['filter_coords'] = Checkbox(
            value=True,
            indent=False,
            description='Hide coordinates',
        )
        self.widgets['filter_restarts'] = Checkbox(
            value=True,
            indent=False,
            description='Hide restarts',
        )

        super().__init__(children=list(self.widgets.values()), **kwargs)

        # Call the event handlers to set up filtering etc
        self._filter_eventhandler(None)
        self._selector_eventhandler(None)
        self._set_observes()

    def _set_observes(self):
        """
        Set event handlers
        """
        for w in ['filter_coords', 'filter_restarts']:
            self.widgets[w].observe(self._filter_eventhandler, names='value')

        self.widgets['search'].observe(self._search_eventhandler, names='value')
        self.widgets['selector'].observe(self._selector_eventhandler, names='value')

    def _filter_eventhandler(self, event=None):
        """
        Optionally hide some variables
        """

        # Set up a mask with all true values
        mask = self.variables.name.ne('')

        # Filter out restarts and coordinates if checkboxes selected 
        if self.widgets['filter_restarts'].value:
            mask = mask & (self.variables['restart'] != self.widgets['filter_restarts'].value)
        if self.widgets['filter_coords'].value:
            mask = mask & (self.variables['coordinate'] != self.widgets['filter_coords'].value)
        
        # Mask out hidden variables
        self.variables['visible'] = mask

        # Update the variable selector
        self._update_variables(self.variables[self.variables.visible])

        # Wipe the search
        self.widgets['search'].value = ''
        self.widgets['selector'].value = None

    def _search_eventhandler(self, event=None):
        """
        Live search bar, updates the selector options dynamically, does not alter
        visible mask in variables
        """
        search_term = self.widgets['search'].value

        variables = self.variables[self.variables.visible]
        if search_term is not None or search_term != '':
            variables = variables[variables.name.str.contains(search_term, na=False) |
                                  variables.long_name.str.contains(search_term, na=False) ]

        self._update_variables(variables)
    
    def _selector_eventhandler(self, event=None):
        """
        Update variable info when variable selected
        """
        style = '<style>p{word-wrap: break-word}</style>' 
        selected_variable = self.widgets['selector'].label
        if selected_variable is None or selected_variable == '':
            long_name = ''
        else:
            # long_name = self.variables[self.variables.name == selected_variable].long_name.values[0]
            long_name = self.widgets['selector'].value
        self.widgets['info'].value = style + '<p><b>Long name:</b> {long_name}</p>'.format(long_name=long_name)
    
    def _update_variables(self, variables):
        """
        Update the variables visible in the selector
        """
        self.widgets['selector'].options = dict(variables.sort_values(['name'])[['name','long_name']].values)

    def delete(self, variable_names=None):
        """
        Remove variables
        """
        # If no variable specified just delete the currently selected one
        if variable_names is None:
            if self.widgets['selector'].label is None:
                return None
            else:
                variable_names = [ self.widgets['selector'].label, ]

        if isinstance(variable_names, str):
            variable_names = [ variable_names, ]

        mask = self.variables['name'].isin(variable_names)
        deleted = self.variables[mask]

        # Delete variables
        self.variables = self.variables[~mask]

        # Update selector. Use search eventhandler so the selector preserves any 
        # current search term. It is annoying to have that reset and type in again 
        # if multiple variables are to be added
        self._search_eventhandler()

        return deleted

    def add(self, variables):
        """
        Add variables
        """
        # Concatenate existing and new variables
        self.variables = pd.concat([self.variables, variables])

        # Need to recalculate the visible flag as new variables have been added
        self._filter_eventhandler(None)


class VariableSelectFilter(widgets.HBox):
    """
    Combo widget which contains a VariableSelector from which variables can 
    be transferred to another Select Widget to specify which variables should
    be used to filter experiments
    """

    variables = pd.DataFrame()
    widgets = {}
    subwidgets = {}
    buttons = {}

    def __init__(self, selvariables, **kwargs):
        """
        selvariables is a dataframe and is used to populate the VariableSelector

        self.variables contains the variables transferred to the selected widget
        """

        layout = {'padding': '0px 5px'}

        # Variable selector combo-widget
        self.widgets['selector'] = VariableSelector(selvariables, **kwargs)

        # Button to add variable from selector to selected
        self.buttons['var_filter_add'] = Button(
            tooltip='Add selected variable to filter',
            icon='angle-double-right',
            layout={'width': 'auto'},
        )
        # Button to add variable from selector to selected
        self.buttons['var_filter_sub'] = Button(
            tooltip='Remove selected variable from filter',
            icon='angle-double-left',
            layout={'width': 'auto'},
        )
        self.widgets['button_box'] = VBox(list(self.buttons.values()), 
                                          layout={'padding': '100px 5px', 'height': '100%'})

        # Selected variables for filtering with header widget
        self.subwidgets['var_filter_label'] = HTML('Filter variables:', layout=layout)
        self.subwidgets['var_filter_selected'] = Select(
            options=[],
            rows=10,
            layout=layout,
        )
        self.widgets['filter_box'] = VBox(list(self.subwidgets.values()), layout=layout)

        super().__init__(children=list(self.widgets.values()), **kwargs)

        self._set_observes()

    def _set_observes(self):
        """
        Set event handlers
        """
        self.buttons['var_filter_add'].on_click(self._add_var_to_selected)
        self.buttons['var_filter_sub'].on_click(self._sub_var_from_selected)

    def _update_variables(self):
        """
        Update filtered variables
        """
        self.subwidgets['var_filter_selected'].options = dict(self.variables.sort_values(['name'])[['name','long_name']].values)

    def _add_var_to_selected(self, button):
        """
        Transfer variable from selector to filtered variables
        """
        self.add(self.widgets['selector'].delete())

    def add(self, variable):
        """
        Add variable to filtered variables
        """
        if variable is None or len(variable) == 0:
            return
        self.variables = pd.concat([self.variables, variable])
        self._update_variables()

    def _sub_var_from_selected(self, button):
        """
        Transfer variable from filtered variables to selector
        """
        self.widgets['selector'].add(self.delete())

    def delete(self, variable_names=None):
        """
        Delete variable from filtered variables
        """
        # If no variable specified just delete the currently selected one
        if variable_names is None:
            if self.subwidgets['var_filter_selected'].label is None:
                return None
            else:
                variable_names = [self.subwidgets['var_filter_selected'].label]

        if isinstance(variable_names, str):
            variable_names = [ variable_names, ]

        mask = self.variables['name'].isin(variable_names)
        deleted = self.variables[mask]

        # Delete variables
        self.variables = self.variables[~mask]

        # Update selector
        self._update_variables()

        return deleted

    def selected_vars(self):
        """
        Return all the variables in the selected variables box
        """
        return self.subwidgets['var_filter_selected'].options

class DatabaseExplorer(VBox):
    """
    Combo widget based on a select box containing all experiments in
    specified database. 
    """

    session = None
    de = None
    widgets = {}

    def __init__(self, session=None, de=None):

        if de is None: 
            de = DatabaseExplorer(session)
        self.de = de

        self._make_widgets()
        self._set_handlers()

    @staticmethod
    def return_value_or_empty(value):
        """Return value if not None, otherwise empty"""
        if value is None:
            return ''
        else:
            return value

    def _make_widgets(self):

        box_layout = Layout(padding='10px', width='auto', border= '0px solid black')

        # Gui header
        self.widgets['header'] = HTML(
            value="""
            <h3>Database Explorer</h3>

            <p>Select an experiemt to show more detailed information where available.
            With an experiment selected push 'Load' to open an Experiment Explorer gui.

            <p>Select keywords and/or variables and push 'Filter' to show only 
            matching experiments. Use option or shift key to select multiple variables</p>

            </p>
            """,
            description='',
        ) 

        # Experiment selector box
        self.widgets['expt_selector'] = Select(
            options=self.de.experiments.experiment,
            rows=20,
            layout={'width': 'initial'},
            disabled=False
        )

        # Keyword filtering element is a box containing a bunch of
        # checkboxes
        self.widgets['filter_widget'] = VBox(layout={'overflow': 'scroll', 
                                                     'width': 'auto'})
        keywords_checkboxes = [Checkbox(description=str(k), 
                                        value=False, 
                                        indent=False,
                                        layout=box_layout) for k in self.de.keywords]
        self.widgets['filter_widget'].children = keywords_checkboxes

        # Filtering button
        self.widgets['filter_button'] = Button(
            description='Filter',
            layout={'width': '50%', 'align': 'center'},
            tooltip='Click to filter experiments'
        )

        # Variable filter selector combo widget
        self.widgets['var_filter'] = VariableSelectFilter(self.de.variables)

        # Tab box to contain keyword and variable filters
        self.widgets['filter_tabs'] = Tab(title='Filter', children=[self.widgets['filter_widget'], 
                                                                    self.widgets['var_filter']])
        self.widgets['filter_tabs'].set_title(0, 'Keyword')
        self.widgets['filter_tabs'].set_title(1, 'Variable')

        self.widgets['load_button'] = Button(
            description='Load Experiment',
            disabled=False,
            layout={'width': '50%', 'align': 'center'},
            tooltip='Click to load experiment'
        )

        # Experiment information panel
        self.widgets['expt_info'] = HTML(
            value='',
            description='',
            layout={'width': '80%', 'align': 'center'},
        )

        # Some box layout nonsense to organise widgets in space
        selectors = HBox([
                        VBox([Label(value="Experiments:"), 
                              self.widgets['expt_selector'],
                              self.widgets['load_button']],
                              layout={'padding': '10px'}),
                        VBox([Label(value="Filter by:"), 
                              self.widgets['filter_tabs'],
                              self.widgets['filter_button']],
                              layout=box_layout,),
                        ], layout=box_layout
                        )

        # Call super init and pass widgets as children
        super().__init__(children=[self.widgets['header'],
                                   selectors,
                                   self.widgets['expt_info']])

    def _set_handlers(self):
        """
        Define routines to handle button clicks and experiment selection
        """
        self.widgets['expt_selector'].observe(self._expt_eventhandler, names='value')
        self.widgets['load_button'].on_click(self._load_experiment)
        self.widgets['filter_button'].on_click(self._filter_experiments)

    def _filter_restart_eventhandler(selector):
        """
        Re-populate variable list when checkboxes selected/de-selected
        """
        self._filter_variables()

    def _expt_eventhandler(self, selector):
        """
        When experiment is selected populate the experiment information
        elements
        """
        if selector.new is None:
            return
        self._show_experiment_information(selector.new)

    def _show_experiment_information(self, experiment_name):
        """
        Populate box with experiment information
        """
        expt = self.de.experiments[self.de.experiments.experiment == experiment_name]

        style ="""
        <style>
            body  { font: normal 8px Verdana, Arial, sans-serif; }
            table { border-spacing: 8px 0px; background-color: #fff;" }
            td    { padding: 2px; }
        </style>
        """
        self.widgets['expt_info'].value = style + """
        <table>
        <tr><td><b>Experiment:</b></td> <td>{experiment}</td></tr>
        <tr><td style="vertical-align:top;"><b>Description:</b></td> <td>{description}</td></tr>
        <tr><td><b>Notes:</b></td> <td>{notes}</td></tr>
        <tr><td><b>Contact:</b></td> <td>{contact} &lt;{email}&gt;</td></tr>
        <tr><td><b>No. files:</b></td> <td>{nfiles}</td></tr>
        <tr><td><b>Created:</b></td> <td>{created}</td></tr>
        </table>
        """.format(
                   experiment=experiment_name,
                   description=return_value_or_empty(expt.description.values[0]),
                   notes=return_value_or_empty(expt.notes.values[0]),
                   contact=return_value_or_empty(expt.contact.values[0]),
                   email=return_value_or_empty(expt.email.values[0]),
                   nfiles=return_value_or_empty(expt.ncfiles.values[0]),
                   created=return_value_or_empty(expt.created.values[0]),
                   )
        
    def _filter_experiments(self, b):
        """
        Filter experiment list by keywords and variable
        """
        kwds = []
        options = set(self.de.experiments.experiment)

        for kwd in self.widgets['filter_widget'].children:
            # print(kwd)
            if kwd.value:
                kwds.append(kwd.description)
        if len(kwds) > 0:
            options.intersection_update(self.de.keyword_filter(kwds))

        variables = self.widgets['var_filter'].selected_vars()
        if len(variables) > 0:
            options.intersection_update(self.de.variable_filter(variables))

        self.widgets['expt_selector'].options = options
        self.widgets['expt_selector'].value = None

    def _load_experiment(self, b):
        """
        Open an Experiment Explorer UI with selected experiment
        """
        if self.widgets['expt_selector'].value is not None:
            self.ee = ExperimentExplorer(self.session, self.de)
            self.ee.run(experiment=self.widgets['expt_selector'].value)


class ExperimentExplorer():

    session = None
    data = None
    experiment_name = None
    variables = []
    widgets = {}
    handlers = {}

    def __init__(self, session=None, de=None):

        if de is None: 
            de = DatabaseExplorer(session)

        self.de = de

    @staticmethod
    def return_value_or_empty(value):
        """Return value if not None, otherwise empty"""
        if value is None:
            return ''
        else:
            return value

    def make_widgets(self):

        # Header widget
        self.widgets['header'] = widgets.HTML(
            value="""
            <h3>Experiment Explorer</h3>
            
            <p>Select a variable from the list to display metadata information.
            Where appropriate select a date range. Pressing the <b>Load</b> button
            will read the data into an <tt>xarray DataArray</tt> using the COSIMA Cookook. 
            The command used is output and can be copied and modified as required.</p>

            <p>The loaded DataArray is accessible as the <tt>data</tt> attribute 
            of the ExperimentExplorer object.</p> 
            
            <p>The selected experiment can be changed to any experiment present
            in the current database session.</p>
            """,
            description='',
        )
        
        # Variable search box
        self.widgets['var_search'] = Text(
            placeholder='Start typing', 
            description='Search', 
            layout={'width': 'auto'})
            
        # Variable selector element
        self.widgets['var_selector'] = Select(
            options=[],
            rows=20,
            description='Variables:',
            layout={'width': 'auto'}
        )

        # Coordinate variable filter checkbox
        self.widgets['var_filter_coords'] = Checkbox(
            value=True,
            indent=True,
            description='Hide coordinates',
        )

        # Restart variable filter checkbox
        self.widgets['var_filter_restarts'] = Checkbox(
            value=True,
            indent=True,
            description='Hide restarts',
        )
        
        # Experiment selector element
        self.widgets['expt_selector'] = Dropdown(
            options=self.de.experiments.experiment,
            value=self.experiment_name,
            description='Experiment:',
            layout={'width': 'auto'}
        )

        # Date selection widget
        self.widgets['var_daterange'] = widgets.SelectionRangeSlider(
            options=['0000','0001'],
            index=(0,1),
            description='Date range',
            layout={'width': '80%'},
            disabled=True
        )

        # Variable information widget
        self.widgets['var_info'] = widgets.HTML()

        # DataArray information widget
        self.widgets['data_box'] = widgets.HTML()

        # Data load button
        self.widgets['load_button'] = Button(
            description='Load',
            disabled=False,
            layout={'width': '20%', 'align': 'center'},
            tooltip='Click to load data'
        )

        def load_data(b):
            """
            Called when load_button clicked
            """

            data_box = self.widgets['data_box']

            varname = self.widgets['var_selector'].value
            (start_time, end_time) = self.widgets['var_daterange'].value

            load_command = """
            <pre><code>cc.querying.getvar('{expt}', '{var}', session, 
                       start_time='{start}', end_time='{end}')</code></pre>
            """.format(expt=self.widgets['expt_selector'].value, 
                    var=varname,
                    start=str(start_time),
                    end=str(end_time))

            # Interim message to tell user what is happening
            data_box.value = 'Loading data, using following command ...\n\n' + load_command + 'Please wait ... '

            try:
                self.data = cc.querying.getvar(self.experiment_name,
                                        varname,
                                        self.de.session, 
                                        start_time=str(start_time),
                                        end_time=str(end_time))
            except Exception as e:
                data_box.value = 'Error loading variable {} data: {}'.format(varname, e)
                return

            # Update data box with message about command used and pretty HTML
            # representation of DataArray
            data_box.value = 'Loaded data with' + load_command + self.data._repr_html_()

        self.widgets['load_button'].on_click(load_data)
        
        def expt_eventhandler(selector):
            """
            Called when experiment dropdown menu changes
            """
            self.load_experiment(selector.new)

        self.widgets['expt_selector'].observe(expt_eventhandler, names='value')
    
        def var_search_eventhandler(selector):
            """
            Called when text type into variable search box
            """
            # Find all variables with name or long name that
            # contain the search text
            self.widgets['var_selector'].options = self.get_visible_variables(selector.new)
            # Ensure no current selection
            self.widgets['var_selector'].value = None

        self.widgets['var_search'].observe(var_search_eventhandler, names='value')

        def filter_restart_eventhandler(selector):
            """
            Filter restart and coordinate variables when checkboxes
            selected. Called when checkboxes selected/deselected
            """
            # Mask options
            self.widgets['var_selector'].options = self.get_visible_variables() #self.widgets['var_search'].value)
            self.widgets['var_selector'].value = None

        self.widgets['var_filter_restarts'].observe(filter_restart_eventhandler, names='value')
        self.widgets['var_filter_coords'].observe(filter_restart_eventhandler, names='value')
            
        def var_eventhandler(selector):
            """
            Called when variable selected
            """
            variable = self.variables.loc[self.variables['name'] == selector.new]

            # Initialise daterange widget
            self.widgets['var_daterange'].options = ['0000','0000']
            self.widgets['var_daterange'].disabled = True

            if len(variable) == 0:
                return
            
            # Populate variable information
            self.load_var_info(self.return_value_or_empty(variable.long_name.values[0]), 
                               self.return_value_or_empty(variable.frequency.values[0]))

            # Populate daterange widget if variable contains necessary information
            if (variable.time_start.values[0] is not None and variable.time_end.values[0]  is not None and
                variable.frequency.values[0]  is not None and not variable.frequency.values[0] == 'static'):
                self.widgets['var_daterange'].disabled = False
                # Convert human readable frequency to pandas compatigle frequency string 
                freq = re.sub(r'^(\d+) (\w)(\w+)', r'\1\2', str(variable.frequency.values[0]).upper())
                dates = pd.date_range(variable.time_start.values[0], variable.time_end.values[0] , freq=freq)
                self.widgets['var_daterange'].options = [(i.strftime('%Y/%m/%d'), i) for i in dates]                
                self.widgets['var_daterange'].value = (dates[0], dates[-1])

        self.widgets['var_selector'].observe(var_eventhandler, names='value')

    def get_visible_variables(self, variable_name=None):

        # First check masking of coords and restarts 

        # Set up a mask with all true values
        mask = self.de.variables.name.ne('')

        # Filter out restarts and coordinates if checkboxes selected 
        if self.widgets['var_filter_restarts'].value:
            mask = mask & (self.de.variables['restart'] != self.widgets['var_filter_restarts'].value)
        if self.widgets['var_filter_coords'].value:
            mask = mask & (self.de.variables['coordinate'] != self.widgets['var_filter_coords'].value)

        # Make a temporary list of variables
        variables = self.de.variables[mask]

        if variable_name is None or variable_name == '':
            self.widgets['var_search'].value = ''
        else:
            variables = variables[variables.name.str.contains(variable_name, na=False) | 
                                  variables.long_name.str.contains(variable_name, na=False)]

        # Return a sorted list of variables matching the current search criteria
        return sorted(variables.name, key=str.casefold)

    def load_experiment(self, experiment_name):
        """
        When first instantiated, or experiment changed, the variable
        selector widget needs to be refreshed
        """
        self.experiment_name = experiment_name
        self.variables = self.de.get_variables(experiment_name)
        self.load_variables()

    def load_variables(self):
        """
        Populate the variable selector dialog
        """
        
        # Reset the search box
        self.widgets['var_search'].value = ''

        # Mask options
        self.widgets['var_selector'].options = self.get_visible_variables()
        self.widgets['var_selector'].value = None

    def load_var_info(self, long_name, frequency):
        """
        Populate variable information box
        """
    
        css = """
        <style type="text/css">
        #info table{background-color: #fff;}
        #info td{border-width:0px; padding:5px 5px;}
        #info .left{text-align:left;)
        #info .right{text-align:right;)
        </style>
        """
        self.widgets['var_info'].value = css + """
        <table id="info">
        <tr>
            <td class="right">Long name:</td> <td class="left">{long_name}</td>
        </tr>
        <tr>
            <td class="right">Frequency:</td> <td class="left">{frequency}</td>
        </tr>
        </table>
        """.format(long_name=long_name, frequency=frequency)

    def run(self, experiment=None):

        # Check the experiment specified exists in database
        if experiment is not None:
            if self.de.experiments.experiment.str.contains(experiment).any():
                self.experiment_name = experiment
            else:
                experiment = None

        # Default to first experiment
        if experiment is None:
            self.experiment_name = self.de.experiments.iloc[0].name

        self.make_widgets()

        # Set values initial values for experiment selector widget
        self.widgets['expt_selector'].options = self.de.experiments.experiment
        self.widgets['expt_selector'].value = self.experiment_name

        self.load_experiment(self.experiment_name)
        self.load_var_info('','')

        display(self.widgets['header'])
        
        box_layout = widgets.Layout(position='left', width='auto', padding='10px', flex='1 1 auto', border='0px solid black')
        
        # Left pane
        var_select_box = VBox([
                                self.widgets['var_search'], 
                                self.widgets['var_selector'],
                                self.widgets['var_filter_coords'],
                                self.widgets['var_filter_restarts'],
                               ])
   
        # Right pane
        var_info_box = VBox([
                             self.widgets['expt_selector'],
                             self.widgets['var_info'],
                             self.widgets['var_daterange'],
                             self.widgets['load_button']
                            ], 
                            layout=box_layout
                            )
        # Main dialog
        display(HBox([var_select_box, var_info_box], layout=box_layout))

        # Output box
        display(self.widgets['data_box'])


def VariableExplorer(ds):

    ds.hvplot.quadmesh(datashade=True)
