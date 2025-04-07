from sqlglot import exp

def extract_tables(ast):
    """
    Traverse the AST and extract all table references.
    """
    with_bindings = set()
    tables = set()

    for with_node in ast.find_all(exp.With):
        for with_exp in with_node.expressions:
            with_bindings.add(with_exp.alias)

    for table in ast.find_all(exp.Table):
        tables.add(table.name)

    return tables - with_bindings
