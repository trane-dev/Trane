from trane.ops import (
    FILTER_OPS,
    AllFilterOp,  # noqa
    EqFilterOp,  # noqa
    GreaterFilterOp,  # noqa
    LessFilterOp,  # noqa
    NeqFilterOp,  # noqa
    op_from_json,
    op_to_json,
)
from trane.ops.aggregation_ops import (
    AGGREGATION_OPS,
    AvgAggregationOp,  # noqa
    CountAggregationOp,  # noqa
    MajorityAggregationOp,  # noqa
    MaxAggregationOp,  # noqa
    MinAggregationOp,  # noqa
    SumAggregationOp,  # noqa
)

ALL_OPS = AGGREGATION_OPS + FILTER_OPS


def test_save_load_before_op_type_check():
    """
    Go through each op, check op_to_json and op_from_json work properly before op_type_check
    """
    for op_type in ALL_OPS:
        op = globals()[op_type]("col")
        op_json = op_to_json(op)
        assert isinstance(op_json, str)
        op2 = op_from_json(op_json)
        assert isinstance(op2, globals()[op_type])
        assert op2.column_name == "col"
        assert op2.input_type is None and op2.output_type is None
        assert (
            isinstance(op2.hyper_parameter_settings, dict)
            and len(op2.hyper_parameter_settings) == 0
        )


# def test_save_load_after_op_type_check():
#     """
#     Go through each op, check op_to_json and op_from_json work properly after op_type_check
#     """
#     for op_type in ALL_OPS:
#         meta = TM(
#             {
#                 "tables": [
#                     {
#                         "fields": [
#                             {
#                                 "name": "col",
#                                 "type": TM.SUPERTYPE[TM.TYPE_FLOAT],
#                                 "subtype": TM.TYPE_FLOAT,
#                             },
#                         ],
#                     },
#                 ],
#             },
#         )
#         op = globals()[op_type]("col")
#         op.op_type_check(meta)
#         op_json = op_to_json(op)
#         assert isinstance(op_json, str)
#         op2 = op_from_json(op_json)
#         assert isinstance(op2, globals()[op_type])
#         assert op2.column_name == "col"
#         assert op2.output_type == op.output_type
#         assert isinstance(op2.hyper_parameter_settings, dict)
