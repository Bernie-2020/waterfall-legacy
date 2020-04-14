import collections
import re
from collections import Iterable
import yaml
import pyperclip


def create_importer(table_name, importer_type='base',comments=True,string_file='strings.yml', **kwargs):
    if 'ids' in kwargs and 'mob_id_in' not in kwargs:
        kwargs['mob_id_in'] = kwargs['ids']

    if 'support_ints' in kwargs:
        kwargs['support_ints'] = value_to_comma_string(kwargs['support_ints'])

    if 'capacity' in kwargs and 'capacity_override' not in kwargs:
        kwargs['capacity_override'] = kwargs['capacity']

    params = {
        'support_ints': '1',  # goes in IN statement
        'distance': 'NULL',
        'capacity_override': 'NULL',
        'assumed_acceptance_rate': 0.01,
        'active_event_filters': 'true',
        'table_name': table_name,
        'rows_statement': None
    }

    params.update(kwargs)

    if importer_type.lower() in ('mobilize'):
        importer_type = 'base'
    return MobilizeImporter(params, importer_type.lower(), comments, string_file)


class MobilizeImporter(object):

    def __init__(self, params, prefix='base', comments=True, string_file='strings.yml'):
        self.params = params
        self.prefix=prefix
        self.comments=comments
        with open(string_file, 'r') as yml:
            self.strings = yaml.safe_load(yml.read())

        if 'string_replacements' in self.params:
            for k, v in self.strings.items():
                for old, new in self.params['string_replacements']:
                    self.strings[k] = v.replace(old, new)

        self.make_where()
        self.make_overrides()
        self.make_exclusions()
        self.make_helper_table()

    def get_str(self, key, prefix):
        if key in self.strings:
            return self.strings[key]
        elif (prefix + '_' + key) in self.strings:
            return self.strings[prefix + '_' + key]
        elif prefix+'_parent_importer' in self.strings:
            return self.get_str(key, prefix=self.get_str('parent_importer', prefix))
        elif ('base_' + key) in self.strings:
            return self.get_str(key, prefix='base')
        else:
            return None

    def make_overrides(self):
        overrides_sql = ''
        for override in ['venue_override', 'title_override', 'combo_override']:
            if override not in self.overrides:
                continue
            if override in self.params:
                overrides_sql += ',' + self.overrides[override][0].format(**self.params)+'\n'
            else:
                overrides_sql += ',' + self.overrides[override][1].format(**self.params)+'\n'

        self.params['overrides_sql'] = overrides_sql

    def make_where(self):
        wc: dict = self.where_criteria
        where_sql = ''
        for key, value in wc.items():
            if key in self.params:
                if '_in' in key:
                    self.params[key] = value_to_comma_string(self.params[key])
                where_sql += value.format(**self.params) + '\n'
            elif self.comments:
                where_sql += '--'+value + '\n'
        self.params['where_sql'] = where_sql

    def make_exclusions(self):
        exclusions = self.params['exclusions'] if 'exclusions' in self.params else None
        if exclusions:
            exclusions_sql_inner = '\n\tunion all \n'.join(
                (f"\t(select right(cell,10)::bigint as phone from {x})" for x in exclusions)
            )
            exclusions_sql = f'''left join (
            {exclusions_sql_inner}
        ) exclusions on exclusions.phone = users.phone
        where exclusions.phone is null
        '''
        else:
            exclusions_sql = ''
        self.params.update({'exclusions_sql': exclusions_sql})

    def make_helper_table(self):

        if not self.get_str('helper_table', self.prefix):
            return
        if 'rows' not in self.params or not len(self.params['rows']):
            raise RuntimeError("helper_table True but no rows or empty rows in params")
        rows = self.params['rows']
        self.params['rows_statement'] = '({})'.format('),\n('.join((','.join((str(x) for x in row)) for row in rows)))


    def __getattr__(self, item):
        ret = self.get_str(item, self.prefix)
        if ret:
            if isinstance(ret, str):
                return ret.format(**self.params)
            return ret
        elif item in self.params:
            return self.params[item]
        else:
            raise AttributeError("'{}' has no attribute '{}'".format(self.__class__.__name__, item))

    def full_query(self, copy=False):
        full_query = ''
        for key in self.keys:
            full_query += self.__getattr__(key)
        if copy:
            pyperclip.copy(full_query)
        return full_query


def parse_multi_mobilize(long_str, pattern=r'.*/(?:event|mobilize)/(\d+)/?'):
    return re.findall(pattern, long_str, re.M)


def value_to_comma_string(val):
    if not isinstance(val, str):
        if isinstance(val, Iterable):
            val = ','.join(map(str, val))
        else:
            val = str(val)
    return val

