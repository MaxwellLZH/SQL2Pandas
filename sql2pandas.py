import pandas as pd
import os
import sqlparse
from collections import defaultdict



pd_join_syntax = {
    'LEFT JOIN': 'left',
    'RIGHT JOIN': 'right',
    'OUTER JOIN': 'outer',
    'INNER JOIN': 'inner',
    'JOIN': 'inner'
}



class Table:

    def __init__(self, identifier=None):
        if identifier is not None:
            if not self._validate(identifier):
                raise ValueError('Not a valid table object')
            else:
                self.name = identifier.get_real_name()
                self.alias = identifier.get_alias()
        else:
            self.name = None
            self.alias = None

    @property
    def alias(self):
        return self._alias

    @property    
    def name(self):
        return self._tbl_name

    @alias.setter
    def alias(self, value):
        self._alias = value

    @name.setter
    def name(self, value):
        self._tbl_name = value

    def __repr__(self):
        return self.alias

    def _validate(self, obj):
        """validate whether an object can be a table"""
        return hasattr(obj, 'get_real_name') and hasattr(obj, 'get_alias')



class JoinedTable(Table):
    
    def __init__(self, identifier=None):
        self.left = None
        self.right = None
        self.relation = None  # join relation
        self.on = set([])  # a set of tuples showing the columns to join on
        super().__init__(identifier)


    def _set_left(self, value):
        if self._validate(value):
            self.left = Table(value)
        elif isinstance(value, Table):
            self.left = value
        
        if self.is_full():
            self.name = self.left.name + '-' + self.right.name


    def _set_right(self, value):
        if self._validate(value):
            self.right = Table(value)
        elif isinstance(value, Table):
            self.right = value

        if self.is_full():
            self.name = self.left.name + '-' + self.right.name
    

    def add_table(self, value):
        if self.is_full():
            raise ValueError('Already have two tables.')
        elif self.left is None:
            self._set_left(value)
        else:
            self._set_right(value)


    def set_relation(self, value):
        if not value.is_keyword:
            raise ValueError('Wrong token')
        elif value.normalized not in ['LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'JOIN', 'OUTER JOIN']:
            raise ValueError('Operation type not supported')
        self.relation = value.normalized


    def set_criteria(self, value):
        tbl_name, col_name = parse_comparison(value)
        self_tbl_order = (self.left.alias, self.right.alias)
        if (self_tbl_order[0] == tbl_name[0]) or (self_tbl_order[1] == tbl_name[1]):
            self.on.add(col_name)
        else:
            self.on.add((col_name[1], col_name[0]))


    def to_pandas(self):
        """
        Generate a string for execution
        """
        if not (self.is_full() and self.relation and self.on):
            raise ValueError('Not a complete node.')

        left_cols = "', '".join([x[0] for x in self.on])
        right_cols = "', '".join([x[1] for x in self.on])
        #TODO: fixing the prefix stuff
        skel = """{} = {}.merge('{}', how='{}', left_on=['{}'], right_on=['{}'])
        """
        return skel.format(self.name, self.left.name, self.right.name, 
            pd_join_syntax[self.relation], left_cols, right_cols)

        
    def is_empty(self):
        return self.left is None and self.right is None


    def is_full(self):
        return self.left and self.right



def parse_comparison(c):
    """
    Returns:
        a list of [table name tuple, column name tuple]
    """
    if not isinstance(c, sqlparse.sql.Comparison):
        raise ValueError('Excepting a Comparison type, get {}.'.format(type(c)))
    sublists = list(c.get_sublists())
    assert len(sublists) == 2, 'More than two elements in the comparison'
    l, r = sublists
    return [(l.get_parent_name(), r.get_parent_name()), (l.get_real_name(), r.get_real_name())]


def parse_where(w):
    """
    Returns:
        a list of four element tuple with (tbl_name, col_name, relation, value)
    """
    clauses = []
    sublists = list(w.get_sublists())

    def _parse_sublist(sublist):
        tbl_name, col_name, relation, value = None, None, None, None
        for s in sublist.flatten():
            if s.is_whitespace or s.ttype == sqlparse.tokens.Token.Punctuation:
                continue

            elif s.ttype == sqlparse.tokens.Token.Name:
                if not tbl_name:
                    tbl_name = s.value
                else:
                    col_name = s.value

            elif s.ttype == sqlparse.tokens.Token.Operator.Comparison:
                relation = s.value

            else:
                #determine the value type
                if s.ttype in sqlparse.tokens.Token.Number:
                    value = float(s.value)
                else:
                    #get rid of quotes
                    value = s.value.replace("'", '').replace('"', '')
        return tbl_name, col_name, relation, value
            

    return [_parse_sublist(s) for s in sublists]




query = """
select a.col_a, b.* from
table_one a left join table_two b
on a.common_col = b.common_col and a.col2 = b.col2
where a.age > 6 and b.sex = 'male'
"""

statement = sqlparse.parse(query.strip())[0]

for i, s in enumerate(statement):
    print(i, type(s), s.value)


var_dict = defaultdict(list)
alias_dict = {}

loc = 0
while 1:
    if isinstance(statement[loc], sqlparse.sql.IdentifierList):
        # parsing table names and their used variables in a dict
        for s in statement[loc].get_identifiers():
            var_dict[s.get_parent_name()].append(s.get_real_name())
        loc += 1
    elif not statement[loc].is_keyword:
        loc += 1
        continue
    elif statement[loc].normalized == 'SELECT':
        loc += 1
        continue
    elif statement[loc].normalized == 'FROM':
        loc += 1
        break

# s = statement[20]

# result = parse_where(s)
# print(result)


n = JoinedTable()
n.add_table(statement[10])
n.add_table(statement[6])
n.set_relation(statement[8])
n.set_criteria(statement[14])
n.set_criteria(statement[18])
print(n.to_pandas())

print(n.name)
print(n.alias)


x = JoinedTable()
x.add_table(n)
print(x.left.alias)