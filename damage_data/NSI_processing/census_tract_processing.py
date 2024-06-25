import pandas as pd
import geopandas as gpd
import fiona
import numpy as np
import re
import logging

vuln_curves = pd.read_csv('../vulnerability_curves/nsi_median_vulnerability_curves.csv')

def list_gdb_layers(gdb_path):
    """
    Lists the available layers in a Geodatabase.

    Parameters:
    gdb_path (str): The path to the Geodatabase.

    Returns:
    list: A list of layer names available in the Geodatabase.
    """
    try:
        # Use fiona to open the Geodatabase and list layers
        layers = fiona.listlayers(gdb_path)
        return layers
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def read_gdb_table(gdb_path, table_name, **kwargs):
    """
    Reads a specific table from a Geodatabase into a GeoDataFrame.

    Parameters:
    gdb_path (str): The path to the Geodatabase.
    table_name (str): The name of the table to read.

    Returns:
    GeoDataFrame: A GeoDataFrame containing the data from the table.
    """
    try:
        # Read the table into a GeoDataFrame
        gdf = gpd.read_file(gdb_path, driver='FileGDB', layer=table_name, **kwargs)
        return gdf
    except Exception as e:
        print(f"An error occurred: {e}")
        return gpd.GeoDataFrame()
    

def convert_to_percentage(df, columns):
    """
    Convert specified columns of a dataframe to percentages of the sum across a row.

    Parameters:
    df (pd.DataFrame): The input dataframe.
    columns (list): The list of columns to be converted.

    Returns:
    pd.DataFrame: A new dataframe with specified columns converted to percentages.
    """
    df_copy = df.copy()
    row_sums = df_copy[columns].sum(axis=1)
    for column in columns:
        df_copy[column] = df_copy[column] / row_sums
    return df_copy

def filter_columns_by_prefix(df, prefixes):
    """
    Filter columns of a dataframe that start with one of the specified prefixes.

    Parameters:
    df (pd.DataFrame): The input dataframe.
    prefixes (list): The list of prefixes to filter columns by.

    Returns:
    pd.DataFrame: A dataframe with columns that start with one of the specified prefixes.
    """
    filtered_columns = [col for col in df.columns if any(col.startswith(prefix) for prefix in prefixes)]
    return df[filtered_columns]


def rename_columns(df, cols):
    new_cols = {col: col.split(" - ")[0] for col in cols}
    return (
        df.rename(columns=new_cols),
        new_cols.values()
    )
    

def remove_trailing_letters(s):
    """
    Remove trailing letters from each string in the list.

    Parameters:
    strings (list of str): List of strings to process.

    Returns:
    list of str: List of strings with trailing letters removed.
    """
    import re
    return re.sub(r'[a-zA-Z]+$', '', s)


def process_dataframes(df1, df2, id_column, column_mapping, column_to_match, column_identifier_regex):
    """
    Get a weighted mean of rows in df2, using weights from the columns of df1

    Parameters:
    df1 Dataframe of weights
    df2 Dataframe to get weighted mean from
    id_column Column to group by
    column_mapping Mapping of columns to vulnerability curve
    column_to_match Column to match on in df2
    column_identifier_regex Regex to identify columns to match on in df2

    Returns:
    list of str: List of strings with trailing letters removed.
    """
    import pandas as pd
    
    def process_row(row, id_column):
        results = []
        for key, value in column_mapping.items():
            perc = row[key]
            matching_rows = df2[df2[column_to_match] == value]
            filtered_columns = [col for col in matching_rows.columns if re.search(column_identifier_regex, col)]
            if not filtered_columns:
                continue
            mean_values = matching_rows[filtered_columns].mean(axis=0)
            weighted_mean = mean_values * perc
            results.append(weighted_mean)
        
        if results:
            combined_df = pd.concat(results, axis=1).sum(axis=1)
            combined_df[id_column] = row[id_column]
            return combined_df
        else:
            return pd.Series()
        
    results = df1.apply(lambda row: process_row(row, id_column), axis=1)
    return results





gdb_path = "/Users/chlowrie/Downloads/National.gdb/National.gdb"
table_name = "BuildingCountByOccupancyCensusTract"
StateAbbr = "VI"
structure_types = ("RES", "COM", "IND", "AGR", "REL", "GOV", "EDU")
ID_COL = "Tract"

gdf = read_gdb_table(gdb_path, table_name)
gdf = gdf[gdf["StateAbbr"] == StateAbbr]
gdf_columns = filter_columns_by_prefix(gdf, structure_types)
gdf, columns = rename_columns(gdf, gdf_columns)

column_mapping_to_nsi = {
    c: remove_trailing_letters(c)
    for c in columns
}
frequencies = convert_to_percentage(gdf, columns)

weighted_vulnerability_curves = process_dataframes(frequencies, vuln_curves, ID_COL, column_mapping_to_nsi, "Occupancy", r'^m\d*\.?\d*$')
# weighted_vulnerability_curves.to_csv(f"../vulnerability_curves/{ID_COL}_weighted_vulnerability_curves.csv")


values = read_gdb_table(gdb_path, "BuildingContentFullReplacementValueByOccupancyCensusTractLevel")
print(values.columns)
values = values[values["StateAbbr"] == StateAbbr]
values_columns = filter_columns_by_prefix(values, structure_types)
values, vcolumns = rename_columns(values, values_columns)

weighted_values = (frequencies[columns] * values[vcolumns]).sum(axis=1).rename("OccupancyWeightedValue")
weighted_values = pd.concat([frequencies[ID_COL], weighted_values], axis=1)

composite_values = pd.merge(weighted_vulnerability_curves, weighted_values, on=ID_COL)
composite_values.to_csv(f"../vulnerability_curves/{StateAbbr}_{ID_COL}_weighted_vulnerability_curves_and_values.csv")

geogs = read_gdb_table(gdb_path, "CensusTract")
geogs = geogs[geogs["StateAbbr"] == StateAbbr]
print(geogs.columns)
geogs = pd.merge(geogs, composite_values, on=ID_COL)
geogs.to_file(f"../vulnerability_curves/{StateAbbr}_{ID_COL}_weighted_vulnerability_curves_and_values.gpkg")

