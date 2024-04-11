import duckdb


def get_ad(col, table):
    ad = duckdb.sql("SELECT distinct " + col + " from " + table)
    return ad.fetchnumpy()[col].tolist()


class Level:
    col_name: str = None
    dimension = None

    def get_ad(self):
        ad = duckdb.sql("SELECT distinct " + self.col_name + " from " + self.dimension.schema.table)
        return ad.fetchnumpy()[self.col_name].tolist()

    def __repr__(self):
        return "Level:"+self.col_name


class Dimension:
    name: str = None
    levels: list[Level] = []
    schema = None

    def __init__(self, name, levels, caller=None):
        self.name = name
        self.schema = caller
        for level in levels:
            lvl = Level()
            lvl.dimension = self
            lvl.col_name = level
            self.levels.append(lvl)

    def __repr__(self):
        return self.name + ":" + str(self.levels)


class Measure:
    col_name: str = None
    agg: str = None

    def __init__(self, col, agg):
        self.col_name = col
        self.agg = agg

    def __repr__(self):
        return self.agg + "(" + self.col_name + ")"


class Schema:
    """
    Defines a MD schema over a flat table
    """
    table = None
    dimensions: dict[str, Dimension] = dict()
    measures: dict[str, Measure] = dict()

    def __init__(self, tbl, dims, meas):
        self.table = tbl
        for m in meas:
            col, fun = m
            self.measures[col] = Measure(col, fun)

        for d_name, levels in dims.items():
            self.dimensions[d_name] = Dimension(d_name, levels, self)

    def __repr__(self):
        return "--- Schema ---\n  Table:" + str(self.table) + "\n  Dims:" + str(self.dimensions) + "\n  Measures:" + str(self.measures) + "\n--- END Schema ---"
