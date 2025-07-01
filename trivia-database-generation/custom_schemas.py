from datetime import date
from enum import Enum
from schema import Schema, And, Optional


class IntType(Enum):
    SMALLINT = 0
    INTEGER = 1


def is_int(int_type: IntType):
    range_min: int
    range_max: int

    match int_type:
        case IntType.SMALLINT:
            range_min = -32768
            range_max = 32767
        case IntType.INTEGER:
            range_min = -2147483648
            range_max = 2147483647
        case _:
            raise ValueError(f"Invalid int type '{type}'")

    return And(int, lambda x: range_min <= x <= range_max)


def varchar(min_length: int, max_length: int):
    return And(str, lambda x: min_length <= len(x) <= max_length)


Episode = Schema(
    [
        {
            Optional("idepisode"): is_int(IntType.INTEGER),
            "air_date": date,
            "season_number": is_int(IntType.INTEGER),
            "episode_number": is_int(IntType.SMALLINT),
        }
    ]
)

Category = Schema(
    [
        {
            Optional("idcategory"): is_int(IntType.INTEGER),
            "name": varchar(0, 80),
            "round": is_int(IntType.SMALLINT),
            "episode_idepisode": is_int(IntType.INTEGER),
        }
    ]
)

Question = Schema(
    [
        {
            Optional("idquestion"): is_int(IntType.INTEGER),
            "clue_value": is_int(IntType.SMALLINT),
            Optional("comment"): str,
            "question": str,
            "answer": str,
            "category_idcategory": is_int(IntType.INTEGER),
        }
    ]
)
