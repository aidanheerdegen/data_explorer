import re

import cosima_cookbook as cc
import ipywidgets as widgets
from ipywidgets import Button, VBox, HBox, Label, Layout, Select
from ipywidgets import SelectMultiple, Text, Textarea, Checkbox
from ipywidgets import interact, interact_manual, AppLayout
import ipywidgets as wid

import pandas as pd

from cosima_cookbook.database import CFVariable, NCFile, NCExperiment, NCVar

from sqlalchemy import func
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
        self.keywords             = cc.querying.get_keywords(session)
        self.expt_variable_map     = self.experiment_variable_map()
        self.variables            = self.unique_variable_list()

    def experiment_variable_map(self):
        """
        Make a pandas table with experiment as the index and columns
        of name, long_name and restart flag.

        Also make lists of unique name/long_name 
        """

        allvars = pd.concat([self.get_variables(expt)
                     for expt in self.experiments.experiment], keys=self.experiments.experiment)

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
        return cc.querying.get_experiments(self.session, keywords=keywords).experiment

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

def DatabaseExplorer(session=None, de=None):

    def return_value_or_empty(value):
        """Return value if not None, otherwise empty"""
        if value is None:
            return ''
        else:
            return value

    if de is None: 
        de = DatabaseExplorer(session)

    expt_selector = Select(
        options=de.experiments.experiment,
        rows=20,
        # description='Experiments:',
        layout={'width': '100%'},
        disabled=False
    )

    box_layout = Layout(position='left', width='initial', border= '1px solid black')

    # Keyword filtering element
    filter_widget = VBox(layout={'overflow': 'auto', 
                                 'width': 'initial', 
                                 'border': '1px solid black'})
    keywords_checkboxes = [Checkbox(description=str(k), 
                                    value=False, 
                                    indent=False,
                                    layout=box_layout) for k in de.keywords]
    filter_widget.children = keywords_checkboxes

    # Use for outputting debug information
    # output = widgets.Output()
    # Decorate functions to capture stdout and redirect to output widget
    # @output.capture()
    def filter_experiments(b):
        """
        Filter experiment list by keywords and variable
        """
        kwds = []
        options = set(de.experiments.experiment)

        for kwd in keywords_checkboxes:
            # print(kwd)
            if kwd.value:
                kwds.append(kwd.description)
        if len(kwds) > 0:
            options.intersection_update(de.keyword_filter(kwds))

        variables = var_filter_selector.value
        if len(variables) > 0:
            options.intersection_update(de.variable_filter(variables))

        expt_selector.options = options
        expt_selector.value = None

    # Keyword filtering button
    filter_button = Button(
        description='Filter',
        disabled=False,
        layout={'width': '50%', 'align': 'center'},
        button_style='', # 'success', 'info', 'warning', 'danger' or ''
        tooltip='Click to filter experiments'
        #icon='check'
    )
    filter_button.on_click(filter_experiments)
        
    # Element for filtering experiments by variables
    var_filter_selector = SelectMultiple(
        options=sorted(de.variables.name),
        rows=20,
        # description='Experiments:',
        layout={'width': '100%'},
        disabled=False,
    )

    # Experiment information elements
    expt_description = Textarea(
        value='',
        placeholder='Experiment description',
        description='Description',
        layout={'height': '100%', 'width': '100%', 'border': '1px solid black'}
    )
    expt_notes = Textarea(
        value='',
        placeholder='Experiment notes',
        description='Notes',
        layout={'height': '100%', 'width': '100%', 'border': '1px solid black'},
        disabled=False
    )
    
    # Variable filtering elements
    var_filter_coords = Checkbox(
        value = True,
        indent=False,
        description='Hide coordinates',
        layout={'width': '100%', 'border': '1px solid black'},
    )
    var_filter_restarts = Checkbox(
        value = True,
        indent=False,
        description='Hide restarts',
        layout={'width': '100%', 'border': '1px solid black'},
    )

    # Reset button
    def reset_variables(b):
        var_filter_selector.value = ()

    reset_var_button = Button(
        description='Reset',
        disabled=False,
        layout={'width': '50%', 'align': 'center'},
        tooltip='Click to reset variable selection'
    )
    reset_var_button.on_click(reset_variables)
        
    # Element for filtering experiments by variables
    var_filter_selector = SelectMultiple(
        options=sorted(de.variables.name),
        rows=20,
        # description='Experiments:',
        layout={'width': '100%'},
        disabled=False
    )
    variables = None
    
    def expt_eventhandler(selector):
        """
        When experiment is selected populate the experiment information
        elements
        """
        # print(expt_selector.value)
        if selector.new is None:
            return

        show_experiment_information()

    expt_selector.observe(expt_eventhandler, names='value')
    
    def filter_restart_eventhandler(selector):
        """
        When experiment is selected populate the experiment information
        elements
        """
        # Set up a mask with all true values
        mask = de.variables.name.ne('')

        # Filter out restarts and coordinates if checkboxes selected 
        if var_filter_restarts.value:
            mask = mask & (de.variables['restart'] != var_filter_restarts.value)
        if var_filter_coords.value:
            mask = mask & (de.variables['coordinate'] != var_filter_coords.value)

        # Mask options
        var_filter_selector.options = sorted(de.variables[mask].name, key=str.casefold)

    filter_restart_eventhandler(None)

    var_filter_restarts.observe(filter_restart_eventhandler, names='value')
    var_filter_coords.observe(filter_restart_eventhandler, names='value')

    def load_experiment(b):
        """
        Open an Experiment Explorer UI with selected experiment
        """
        if expt_selector.value is not None:
            ExperimentExplorer(expt_selector.value, session, de)

    def show_experiment_information():

        experiment = expt_selector.value
        expt = de.get_experiment(experiment)

        expt_info.value="""
        <h4>Experiment: {experiment}</h4>

        <b>Description:</b> {description}

        <b>Notes:</b> {notes}

        <b>Contact:</b> {contact} <{email}>

        <b>Number of files::</b> {nfiles}

        <b>Created::</b> {created}
        """.format(
                   experiment=experiment,
                   description=return_value_or_empty(expt.description.values[0]),
                   notes=return_value_or_empty(expt.notes.values[0]),
                   contact=return_value_or_empty(expt.contact.values[0]),
                   email=return_value_or_empty(expt.email.values[0]),
                   nfiles=return_value_or_empty(expt.ncfiles.values[0]),
                   created=return_value_or_empty(expt.created.values[0]),
                   )
        # ['experiment', 'contact', 'email', 'created', 'description', 'notes', 'root_dir', 'ncfiles']
    expt_info = widgets.HTML(
        value='',
        description='Experiment Information:',
    )

    load_button = widgets.Button(
        description='Load Experiment',
        disabled=False,
        layout={'width': '20%', 'align': 'center'},
        button_style='', # 'success', 'info', 'warning', 'danger' or ''
        tooltip='Click to load experiment'
        #icon='check'
    )
    load_button.on_click(load_experiment)

    header = widgets.HTML(
        value="""
        <h3>Database Explorer</h3>

        <p>Select keywords and/or variables and push 'Filter' to show only 
        matching experiments. Use option or shift key to select multiple variables</p>

        <p>Select an experiemt to show more detailed information where available.
        With an experiment selected push 'Load' to open an Experiment Explorer gui,
        or copy the experiment name and use in your own code.
        </p>
        """,
        description='',
    )
    
    display(header)
    selectors = HBox(
                    [
                      VBox([
                           Label(value="Experiments:"), 
                           expt_selector,
                           ],
                           layout={'padding': '10px', 'border': '1px solid grey'}
                           ),
                      VBox([
                            Label(value="Filter by keyword:"), 
                            filter_widget, 
                            filter_button,
                            ],
                            layout={'padding': '10px', 'border': '1px solid grey'}
                          ),
                      VBox([
                            Label(value="Filter by variable:"), 
                            var_filter_selector],
                            layout={'padding': '10px', 'border': '1px solid grey'}
                           ),
                      VBox([
                            var_filter_coords,
                            var_filter_restarts, 
                            reset_var_button, 
                            ],
                            layout={'border': '0px solid grey'}
                           )
                    ], layout={'width': '100%', 'padding': '10px', 'border': '1px solid black'}
                    )
    display(selectors)
                              
    info = widgets.HBox([expt_info, ],
                         layout={'width': '100%', 'height': '200px', 'padding': '10px', 'border': '0px solid black'}
                   )
    #display(output)
    display(info)
    load_box = widgets.HBox([load_button,])
    display(load_box)

def ExperimentExplorer(experiment, session=None, de=None):

    def return_value_or_empty(value):
        """Return value if not None, otherwise empty"""
        if value is None:
            return ''
        else:
            return value

    if session is None:
        session = cc.database.create_session()

    # Variable search box
    var_search = widgets.Text(
        value='',
        disabled=False,
        placeholder='Start typing',
        description='Search',
        layout={'width': '90%'}
    )
        
    # Variable selector element
    var_selector = widgets.Select(
        options=[],
        rows=20,
        description='Variables:',
        layout={'width': 'initial'}
    )
    
    def load_experiment(expt_name):
        expt = cc.querying.get_experiments(session, all=True).query(
            'experiment == "{experiment}"'.format(experiment=expt_name)
        )
        variables = cc.querying.get_variables(session, expt_name)
        var_selector.options = variables.name
        var_selector.value = None
        return variables
    
    variables = load_experiment(experiment)

    box_layout = widgets.Layout(position='left', width='initial', flex='1 1 auto', border= '1px solid black')

    
    # Experiment selector element
    expt_selector = widgets.Dropdown(
        options=cc.querying.get_experiments(session).experiment,
        value=experiment,
        description='Experiment:',
        layout={'width': 'initial'}
    )
    # Variable information elements
    var_longname = widgets.Text(
        value='',
        disabled=True,
        placeholder='Long variable name',
        description='Long name',
        layout={'width': '90%'}
    )
    var_frequency = widgets.Text(
        value='',
        disabled=True,
        placeholder='',
        description='Frequency',
        layout={'width': '90%'}
    )
    var_daterange = widgets.SelectionRangeSlider(
        options=['0000','0001'],
        index=(0,1),
        description='Date range',
        layout={'width': '80%'},
        disabled=True
    )
    
    def expt_eventhandler(selection):
        load_experiment(selection.new)
  
    def var_search_eventhandler(selector):
        var_selector.options = variables[variables.name.str.contains(selector.new, na=False) 
                  | variables.long_name.str.contains(selector.new, na=False)].name
        
    def var_eventhandler(selector):
        variable = variables.loc[variables['name'] == selector.new]
        var_daterange.options = ['0000','0000']
        var_daterange.disabled = True
        var_longname.value = ''
        var_frequency.value = ''
        if len(variable) == 0:
            return
        
        if (variable.time_start.values[0] is not None and 
            variable.time_end.values[0]  is not None and
            variable.frequency.values[0]  is not None and
            not variable.frequency.values[0] == 'static'):
            var_daterange.disabled = False
            freq = re.sub(r'^(\d+) (\w)(\w+)', r'\1\2', str(variable.frequency.values[0]).upper())
            dates = pd.date_range(variable.time_start.values[0], variable.time_end.values[0] , freq=freq)
            var_daterange.options = [(i.strftime('%Y/%m/%d'), i) for i in dates]                
            var_daterange.value = (dates[0], dates[-1])
                
        var_longname.value = return_value_or_empty(variable.long_name.values[0])
        var_frequency.value = return_value_or_empty(variable.frequency.values[0])
            
    var_search.observe(var_search_eventhandler, names='value')
    var_selector.observe(var_eventhandler, names='value')
    expt_selector.observe(expt_eventhandler, names='value')


    header = widgets.HTML(
        value="""
        <h3>Experiment Explorer</h3>
        
        <p>Select a variable from the list to display metadata information.
        Where appropriate select a date range. Pressing the <load> button
        will read the data into an xarray DataArray variable <tt>ds</tt> 
        using the COSIMA Cookook, and the command used will be output and
        can be copied and modified as required.</p>
        
        <p>The selected experiment can be changed to any experiment present
        in the current database session<p>
        """,
        description='',
    )
    
    display(header)
    
    data_box = widgets.HTML()

    # Data loading button
    def load_data(b):
        varname = var_selector.value
        (start_time, end_time) = var_daterange.value
        print("""ds = cc.querying.getvar('{expt}', 
                               '{var}',
                               session,
                               start_time='{start}',
                               end_time='{end}')
       """.format(expt=expt_selector.value, 
                  var=var_selector.value,
                  start=str(start_time),
                  end=str(end_time)))
        ds = cc.querying.getvar(expt_selector.value, 
                                var_selector.value, 
                                session, 
                                start_time=str(start_time),
                                end_time=str(end_time))
        data_box.value=ds._repr_html_()

    load_button = widgets.Button(
        description='Load',
        disabled=False,
        layout={'width': '20%', 'align': 'center'},
        button_style='', # 'success', 'info', 'warning', 'danger' or ''
        tooltip='Click to load data'
        #icon='check'
    )
    load_button.on_click(load_data)
    
    var_info = widgets.VBox(
                        [expt_selector,
                         var_longname,
                         var_frequency,
                         var_daterange,
                         load_button
                        ], 
                        layout=box_layout
                        )
    display(widgets.HBox([widgets.VBox([var_search, var_selector]), var_info], layout=box_layout))
    display(data_box)


def VariableExplorer(ds):

    ds.hvplot.quadmesh(datashade=True)
