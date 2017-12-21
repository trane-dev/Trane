import pandas as pd
import json
from ..utils.table_meta import TableMeta
from ..ops import op_saver

__all__ = ['PredictionProblem']

class PredictionProblem:

    """
    Prediction Problem is made up of a list of Operations. The list of operations delineate
    the order the operations will be applied in.
    """

    def __init__(self, table_meta, operations, label_generating_column, entity_id_column, time_column):
        """
        Args:
            (List) Operations: a list of operations (class Operation) that define the
                order in which operations should take place.
            (String) label_generating_column: the column of interest. This column
                will be solely used for performing operations against.
            (String) entity_id_column: the column with entity id's.
            (String) time_column: the name of the column containing time information.
        Returns:
            None
        """
        self.table_meta = table_meta.copy()
        self.operations = operations
        self.label_generating_column = label_generating_column
        self.entity_id_column = entity_id_column
        self.time_column = time_column
        
        self.valid = True
        temp_meta = self.table_meta.copy()
        for op in operations:
            temp_meta = op.preprocess(temp_meta)
            if not temp_meta:
                self.valid = False
                break
        
    def execute(self, dataframe):
        """
        This function executes all the operations on the dataframe and returns the output. The output
        should be structured as a single label/value per the Trane documentation.
        See paper: "What would a data scientist ask? Automatically formulating and solving predicton
        problems."
        Args:
            (Pandas DataFrame): the dataframe containing the data we wish to analyze.
        Returns:
            (Boolean/Float): The Label/Value of the prediction problem's formulation when applied to the data.
        """
        df_groupby = dataframe.copy().groupby(self.entity_id_column)
        outputs = [df_groupby.get_group(key) for key in df_groupby.groups.keys()]
        for operation in self.operations:
            for i in range(len(outputs)):
                if outputs[i].shape[0] > 0:
                    outputs[i] = operation.execute(outputs[i])
                    if outputs[i] is None:
                        print(str(operation))
                        assert 0
        output = pd.concat(outputs)
        output = output[[self.entity_id_column, self.label_generating_column]]
        return output
    
    def __str__(self):
        """
        Args:
            None
        Returns:
            A natural language text describing the prediction problem.
        """
        description = ""
        last_op_idx = len(self.operations) - 1
        for idx, operation in enumerate(self.operations):
            description += str(operation)
            if idx != last_op_idx:
                description += "->"
        return description

    def get_entity_ids(self):
        """
        A method to get all unique entity id's.
        Args:
            None
        Returns:
            (Set) A set of unique entity id's
        """
        return set(dataframe[self.entity_id_column])

    def determine_cutoff_time(self, dataframe):
        """
        This function generates the cutoff times for each entity id and puts the dictionary in the
            prediction problem (self).
        Current implementation is simple. The cutoff time is halfway through the observed
            time period for each entity.
        Args:
            (Pandas DataFrame): the dataframe containing the data we wish to analyze.)
        Returns:
            (Dict): Entity Id to Cutoff time mapping.
        """
        unique_entity_ids = self.get_entity_ids()
        entity_id_to_cutoff_time = {}
        for entity_id in unique_entity_ids:
            df_entity_id = dataframe[dataframe[self.entity_id_column] == entity_id]
            first_time_observed = df_entity_id[self.time_column].min()
            last_time_observed = df_entity_id[self.time_column].max()
            total_time = last_time_observed - first_time_observed
            cutoff_time = first_time_observed + total_time/2.
            entity_id_to_cutoff_time[entity_id] = cutoff_time

        return entity_id_to_cutoff_time

    def set_global_cutoff_time(self, global_cutoff_time):
        """
        A method to set the cutoff time for all entity id's.
        Args:
            (??): A global cutoff_time
        Returns:
            None
        """
        unique_entity_ids = self.get_entity_ids()
        entity_id_to_cutoff_time = {}
        for entity_id in unique_entity_ids:
            entity_id_to_cutoff_time[entity_id] = global_cutoff_time
        self.entity_id_to_cutoff_time = entity_id_to_cutoff_time

    def get_entity_id_to_cutoff_time(self):
        return self.entity_id_to_cutoff_time
        
    def generate_nl_description(self):
        return "For each %s,%s predict%s%s%s." % (self.entity_id_column, 
                self.operations[0].generate_nl_description(), self.operations[3].generate_nl_description(), 
                self.operations[2].generate_nl_description(), self.operations[1].generate_nl_description())

    def to_json(self):
        return json.dumps(
        {"table_meta": json.loads(self.table_meta.to_json()),
        "operations": [json.loads(op_saver.to_json(op)) for op in self.operations],
        "label_generating_column": self.label_generating_column,
        "entity_id_column": self.entity_id_column,
        "time_column":self.time_column
        }
        )
        
    def from_json(json_data):
        data = json.loads(json_data)
        table_meta = TableMeta.from_json(json.dumps(data['table_meta']))
        operations = [op_saver.from_json(json.dumps(item)) for item in data['operations']] 
        label_generating_column = data['label_generating_column']
        entity_id_column = data['entity_id_column']
        time_column = data['time_column']
        return PredictionProblem(table_meta, operations, label_generating_column, entity_id_column, time_column)
    
        
