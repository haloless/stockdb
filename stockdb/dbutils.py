
import pandas as pd


def rename_df_cols(df, fields, name=None):
    """
    df: pandas data frame
    """
    newcols = {}
    for f in fields:
        oldname = name(f) if name else str(f)
        newcols[oldname] = f.showname
    return df.rename(columns=newcols)
####
