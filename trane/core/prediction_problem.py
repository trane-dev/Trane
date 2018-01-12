import pandas as pd
import json
from ..utils.table_meta import TableMeta
from ..ops import op_saver
from dateutil import parser

__all__ = ['PredictionProblem']

class PredictionProblem:

    """
    Prediction Problem is made up of a list of Operations. The list of operations delineate
    the order the operations will be applied in.
    """

    def __init__(self, operations):
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
        self.operations = operations
        
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
        dataframe = dataframe.copy()
        if type(self.cutoff_time) == str:
            cutoff = [parser.parse(item) > parser.parse(self.cutoff_time) for item in dataframe[self.time_column]]
            dataframe = dataframe[cutoff]
        else:
            dataframe = dataframe[dataframe[self.time_column] > self.cutoff_time]
        df_groupby = dataframe.groupby(self.entity_id_column)
        outputs = [df_groupby.get_group(key) for key in df_groupby.groups.keys()]
        for operation in self.operations:
            for i in range(len(outputs)):
                if outputs[i].shape[0] > 0:
                    outputs[i] = operation.execute(outputs[i])
                    if outputs[i] is None:
                        print(str(operation))
                        assert 0
        for item in outputs:
            item["trane_cutoff"] = self.cutoff_time
        output = pd.concat(outputs)
        output = output[[self.entity_id_column, "trane_cutoff", self.label_generating_column]]
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
        
    def to_json(self):
        return json.dumps(
            {"operations": [json.loads(op_saver.to_json(op)) for op in self.operations]})
        
    def from_json(json_data):
        data = json.loads(json_data)
        operations = [op_saver.from_json(json.dumps(item)) for item in data['operations']] 
        return PredictionProblem(operations)
    
        
