from schema import get_ad


def get_assess(reference, assessed, val1, val2, measure, table, function='sum'):
    return "select t1." + reference + ", " + \
        "       t1.measure1 , t2.measure2  " + \
        "from " + \
        "  (select " + assessed + ", " + reference + ", " + function + "(" + measure + ") as measure1 " + \
        "   from " + table + " " + \
        "   where  " + assessed + " = '" + val1 + "' " + \
        "   group by " + assessed + ", " + reference + ") t1, " + \
        "  (select " + assessed + ", " + reference + "," + function + "(" + measure + ") as measure2 " + \
        "   from " + table + " " + \
        "   where " + assessed + " = '" + val2 + "' " + \
        "   group by " + assessed + ", " + reference + ") t2 " + \
        "where t1." + reference + " = t2." + reference + ";"


def get_all_assess(reference, assessed, measure, table, function='sum', ref=None):
    # get active domain
    vals = get_ad(assessed, table)
    if ref is not None:
        r = list(filter(lambda x: x == ref, vals))[0]
        vals = [r] + list(filter(lambda x: x != ref, vals))
    # build projection one column pre value in ad off assessed
    s = "select t1." + reference + ", "
    nb_vals = len(vals)
    for i, val in enumerate(vals):
        s += "t" + str(i) + '."' + val + '"'
        if ref is not None and val == r:
            s += " as '__ref__" + val + "' "
        if i < nb_vals - 1:
            s += ","
    s += "from "
    # build aggregates
    for i, val in enumerate(vals):
        s += "  (select " + assessed + ", " + reference + ", " + function + "(" + measure + ") as " + '"' + val + '"' + \
             "   from " + table + " " + \
             "   where  " + assessed + " = '" + val + "' " + \
             "   group by " + assessed + ", " + reference + ") t" + str(i)
        if i < nb_vals - 1:
            s += ","
    # build join (inner)
    s += " where "
    for i in range(nb_vals - 1):
        s += "t" + str(i) + "." + reference + "=" + "t" + str(i + 1) + "." + reference
        if i < nb_vals - 2:
            s += " AND "
    return s + ";"


def get_all_assess_outer(reference, assessed, measure, table, function='sum', ref=None):
    # get active domain
    vals = get_ad(assessed, table)
    if ref is not None:
        r = list(filter(lambda x: x == ref, vals))[0]
        vals = [r] + list(filter(lambda x: x != ref, vals))
    # build projection one column pre value in ad off assessed
    s = "select t1." + reference + ", "
    nb_vals = len(vals)
    for i, val in enumerate(vals):
        s += "t" + str(i) + '."' + val + '"'
        if ref is not None and val == r:
            s += " as '__ref__" + val + "' "
        if i < nb_vals - 1:
            s += ","
    s += "from "
    # build aggregates
    for i, val in enumerate(vals):
        s += "  (select " + assessed + ", " + reference + ", " + function + "(" + measure + ") as " + '"' + val + '"' + \
             "   from " + table + " " + \
             "   where  " + assessed + " = '" + val + "' " + \
             "   group by " + assessed + ", " + reference + ") t" + str(i)
        if i == 0:
            s += " LEFT OUTER JOIN "
        if i > 0 and i < nb_vals:
            s += " ON " + "t" + str(i) + "." + reference + "=" + "t" + str(i - 1) + "." + reference
            if i < nb_vals - 1:
                s += " LEFT OUTER JOIN "
    return s + ";"
