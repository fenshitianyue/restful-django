#!/usr/bin/python2

def process_table_name(raw_name):
    pos = raw_name.find('.')
    if pos != -1:
        schema = raw_name[:pos]
        table_name = raw_name[pos+1:]
        schema = '"' + schema + '"'
        table_name = '"' + table_name + '"'
        return schema + '.' + table_name
    else:
        return raw_name


if __name__ == '__main__':
    need_table_name = '"sngapm"."df_log"'
    table_name = 'sngapm.df_log'
    table_name = process_table_name(table_name)

    if table_name == need_table_name:
        print 'successfully...'
    else:
        print 'error!'


