# -*- coding:UTF-8 -*-

import inspect
from api import models as api_models

table_map = dict()

for (name, _) in inspect.getmembers(api_models, inspect.isclass):
    table_map.update({getattr(api_models, name)._meta.db_table: name})




