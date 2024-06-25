import copy
import pandas as pd
import numpy as np
import xarray as xr



def apply_ddf(
    ds,
    ddfs="./damage_data/damage/DDF_Global.csv"
):
    ds = copy.deepcopy(ds)
    ddfs = pd.read_csv(ddfs)

    def depth_to_damage_percent(depth):
        return np.interp(depth, ddfs['Depth'], ddfs['Damage'])
    
    return xr.apply_ufunc(depth_to_damage_percent, ds)
