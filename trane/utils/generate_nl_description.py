from .table_meta import TableMeta as TM
from ..ops import *
from .generate_cutoff_times import *

__all__ = ['generate_nl_description']


def generate_nl_description(
        problems, meta, entity_id_column, label_generating_column,
        time_column, cutoff_strategy):
    """This function generates natural language description
    for prediction problems.

    description := For each [entity_id_column], predict 
        [dataop_description] [cutoff_description] [filter_decription]

    dataop_description := 
        the number of records if CountAggregationOp
        the [aggop] [transop] [rowop]

    cutoff_description := after [time_column](threshold)

    filter_description := 
        [empty] if AllFilterOp
        with [filter_column] [op] (threshold)

    Args:
        problems: list of PredictionProblem
        meta: TableMeta
        entity_id_column: str
        label_generating_column: str
        time_column:str

    Returns:
        list of str: natural language descriptions

    """

    def description(prob):
        return "For each {col}, predict{dataop_des}{filter_des}{cutoff_des}.".format(
            col=entity_id_column,
            dataop_des=dataop_description(prob),
            filter_des=filter_description(prob),
            cutoff_des=cutoff_description(prob)
        )

    def dataop_description(prob):
        row_op = prob.operations[1]
        trans_op = prob.operations[2]
        agg_op = prob.operations[3]

        def rowop_description():
            row_op_str_dict = {
                GreaterRowOp: "greater than",
                EqRowOp: "equal to",
                NeqRowOp: "not equal to",
                LessRowOp: "less than"
            }
            if isinstance(row_op, IdentityRowOp):
                return " {col}".format(col=row_op.column_name)
            if isinstance(row_op, ExpRowOp):
                return " the exp of {col}".format(col=row_op.column_name)
            if type(row_op) in row_op_str_dict:
                return " {col} is {op} {threshold}".format(
                    col=row_op.column_name, op=row_op_str_dict[type(row_op)],
                    threshold=row_op.hyper_parameter_settings['threshold'])
            return " (unknown row op)"

        def transop_description():
            if isinstance(trans_op, IdentityTransformationOp):
                return ""
            if isinstance(trans_op, DiffTransformationOp):
                return " the fluctuation of"

        def aggop_description():
            agg_op_str_dict = {
                FirstAggregationOp: "the first",
                LastAggregationOp: "the last",
                CountAggregationOp: "the number of",
                SumAggregationOp: "the sum of",
                LMFAggregationOp: "the last minus first"
            }
            if agg_op.input_type == TM.TYPE_BOOL and isinstance(agg_op, SumAggregationOp):
                return " the number of records whose"
            if agg_op.input_type == TM.TYPE_BOOL:
                return " whether the {op}"
            if type(agg_op) in agg_op_str_dict:
                return " " + agg_op_str_dict[type(agg_op)]

        if type(agg_op) == CountAggregationOp:
            return "{agg_op} records".format(agg_op=aggop_description())
        else:
            return "{agg_op}{trans_op}{row_op}".format(
                agg_op=aggop_description(),
                trans_op=transop_description(),
                row_op=rowop_description()
            )

    def cutoff_description(prob):
        if isinstance(cutoff_strategy, ConstantCutoffTime):
            return ", after {col} {cutoff}".format(col=time_column, cutoff=cutoff_strategy.label_cutoff)
        else:
            raise " (unknown cutoff time)"

    def filter_description(prob):
        filter_op_str_dict = {
            GreaterFilterOp: "greater than",
            EqFilterOp: "equal to",
            NeqFilterOp: "not equal to",
            LessFilterOp: "less than"
        }
        filter_op = prob.operations[0]
        if isinstance(filter_op, AllFilterOp):
            return ""
        return ", with {col} {op} {threshold}".format(
            col=filter_op.column_name,
            op=filter_op_str_dict[type(filter_op)],
            threshold=filter_op.hyper_parameter_settings['threshold'])

    ret = [description(prob) for prob in problems]
    return ret
