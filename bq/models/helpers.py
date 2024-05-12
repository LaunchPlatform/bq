import typing


def make_repr_attrs(items: typing.Sequence[typing.Tuple[str, typing.Any]]) -> str:
    return " ".join(map(lambda item: "=".join([item[0], str(item[1])]), items))
