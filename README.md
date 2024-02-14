![walmartecomm](walmartecomm.jpg)

Walmart is the biggest retail store in the United States. Just like others, they have been expanding their e-commerce part of the business. By the end of 2022, e-commerce represented a roaring $80 billion in sales, which is 13% of total sales of Walmart. One of the main factors that affects their sales is public holidays, like the Super Bowl, Labour Day, Thanksgiving, and Christmas. 

In this project, you have been tasked with creating a data pipeline for the analysis of supply and demand around the holidays, along with conducting a preliminary analysis of the data. You will be working with two data sources: grocery sales and complementary data. You have been provided with the `grocery_sales` table in `PostgreSQL` database with the following features:

# `grocery_sales`
- `"index"` - unique ID of the row
- `"Store_ID"` - the store number
- `"Date"` - the week of sales
- `"Weekly_Sales"` - sales for the given store

Also, you have the `extra_data.parquet` file that contains complementary data:

# `extra_data.parquet`
- `"IsHoliday"` - Whether the week contains a public holiday - 1 if yes, 0 if no.
- `"Temperature"` - Temperature on the day of sale
- `"Fuel_Price"` - Cost of fuel in the region
- `"CPI"` – Prevailing consumer price index
- `"Unemployment"` - The prevailing unemployment rate
- `"MarkDown1"`, `"MarkDown2"`, `"MarkDown3"`, `"MarkDown4"` - number of promotional markdowns
- `"Dept"` - Department Number in each store
- `"Size"` - size of the store
- `"Type"` - type of the store (depends on `Size` column)

You will need to merge those files and perform some data manipulations. The transformed DataFrame can then be stored as the `clean_data` variable containing the following columns:
- `"Store_ID"`
- `"Month"`
- `"Dept"`
- `"IsHoliday"`
- `"Weekly_Sales"`
- `"CPI"`
- "`"Unemployment"`"

After merging and cleaning the data, you will have to analyze monthly sales of Walmart and store the results of your analysis as the `agg_data` variable that should look like:

|  Month | Weekly_Sales  | 
|---|---|
| 1.0  |  33174.178494 |
|  2.0 |  34333.326579 |
|  ... | ...  |  

Finally, you should save the `clean_data` and `agg_data` as the csv files.

It is recommended to use `pandas` for this project. 


```python
-- Write your SQL query here
SELECT * FROM grocery_sales 
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>index</th>
      <th>Store_ID</th>
      <th>Date</th>
      <th>Dept</th>
      <th>Weekly_Sales</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>1</td>
      <td>2010-02-05</td>
      <td>1</td>
      <td>24924.50</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>2010-02-05</td>
      <td>26</td>
      <td>11737.12</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1</td>
      <td>2010-02-05</td>
      <td>17</td>
      <td>13223.76</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>1</td>
      <td>2010-02-05</td>
      <td>45</td>
      <td>37.44</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>1</td>
      <td>2010-02-05</td>
      <td>28</td>
      <td>1085.29</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>231517</th>
      <td>232414</td>
      <td>24</td>
      <td>2011-05-06</td>
      <td>8</td>
      <td>49471.07</td>
    </tr>
    <tr>
      <th>231518</th>
      <td>232415</td>
      <td>24</td>
      <td>2011-05-06</td>
      <td>50</td>
      <td>1210.00</td>
    </tr>
    <tr>
      <th>231519</th>
      <td>232416</td>
      <td>24</td>
      <td>2011-05-06</td>
      <td>87</td>
      <td>25893.32</td>
    </tr>
    <tr>
      <th>231520</th>
      <td>232417</td>
      <td>24</td>
      <td>2011-05-06</td>
      <td>85</td>
      <td>1357.83</td>
    </tr>
    <tr>
      <th>231521</th>
      <td>232418</td>
      <td>24</td>
      <td>2011-05-06</td>
      <td>35</td>
      <td>3648.91</td>
    </tr>
  </tbody>
</table>
<p>231522 rows × 5 columns</p>
</div>




```python


```

 
 - (dataframe )clean_data transform(merged_df) --> fill missing data, add Month Column, keep rows> 10000 sale and drop unnecessary columns
 -  agg_data avg_monthly_sales(clean_data) --> aggerated two columns Month, Avg_sales
 -  load( agg_data, path) --> store in csv file without index
 -  validation() --> checks if two csv files from the load exsit 

Extracting the data


```python

def extract (store_data,extra_data):
    parquet_data = pd.read_parquet(extra_data)
    merged_df = store_data.merge(parquet_data, on = "index")
    return merged_df
    
```

 Clean data


```python
def transform(data):
    data.fillna(
        {'CPI' : data['CPI'].mean(),
         'Weekly_Sales': data['Weekly_Sales'].mean(),
         'Unemployment': data['Unemployment'].mean(),
        }, inplace = True)
    data["Data"] = pd.to_datetime(data["Date"], format = "%Y-%m-%d")
    data["Month"] = data["Date"].dt.month
    
    data = data.loc[data["Weekly_Sales"] > 10000,:]
    data = data.drop(["index", "Temperature", "Fuel_Price", "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5", "Type", "Size", "Date"], axis = 1)
    return data 
    
```

Preliminary analysis


```python
def avg_monthly_sales(data):
    df = data[["Month","Weekly_Sales"]]
    df = (df.groupby("Month").agg(Avg_Sales = ("Weekly_Sales", "mean")).reset_index().round(2))
    #df = pd.concat([column1,column2], ignore_index=True, sort=False)
    return df
    
```

loading and validationg


```python
def load(data,agg, data_path, agg_path):
    data.to_csv(data_path, index =False)
    agg.to_csv(agg_path, index = False)
    
    
    
```

validation


```python
def validation(path):
    file_exists = os.path.exists(path)
    if not file_exists:
        raise Exception(f"There is no file at the path {path}")
    
```


```python
import pandas as pd
import numpy as np
import logging
import os


merged_df = extract(grocery_sales, "extra_data.parquet")

clean_data = transform(merged_df)
print(clean_data.info())
agg_data = avg_monthly_sales(clean_data)
print(agg_data.info())
load(clean_data,agg_data, "clean_data.csv", "agg_data.csv")

validation("clean_data.csv")
validation("agg_data.csv")
```

    <class 'pandas.core.frame.DataFrame'>
    Int64Index: 106231 entries, 0 to 231519
    Data columns (total 8 columns):
     #   Column        Non-Null Count   Dtype         
    ---  ------        --------------   -----         
     0   Store_ID      106231 non-null  int64         
     1   Dept          106231 non-null  int64         
     2   Weekly_Sales  106231 non-null  float64       
     3   IsHoliday     106231 non-null  int64         
     4   CPI           106231 non-null  float64       
     5   Unemployment  106231 non-null  float64       
     6   Data          106206 non-null  datetime64[ns]
     7   Month         106206 non-null  float64       
    dtypes: datetime64[ns](1), float64(4), int64(3)
    memory usage: 7.3 MB
    None
    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 12 entries, 0 to 11
    Data columns (total 2 columns):
     #   Column     Non-Null Count  Dtype  
    ---  ------     --------------  -----  
     0   Month      12 non-null     float64
     1   Avg_Sales  12 non-null     float64
    dtypes: float64(2)
    memory usage: 320.0 bytes
    None
    
