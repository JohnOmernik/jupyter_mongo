# jupyter_mongo
A module to help interaction with Jupyter Notebooks and Mongo DB

------
This is a python module that helps to connect Jupyter Notebooks to various datasets. 
It's based on (and requires) https://github.com/JohnOmernik/jupyter_integration_base 



## Initialization 
----

### Example Inits

#### Embedded mode using qgrid
```
from mongo_core import Mongo
ipy = get_ipython()
mongo_full = Mongo(ipy, debug=False)
ipy.register_magics(mongo_full)
```

