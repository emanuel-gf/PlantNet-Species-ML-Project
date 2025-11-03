import pandas as pd
import numpy as np
import os
import re
    
def big_preprocessing_step(X):
    """
    Apply a serie of preprocessing steps on the dataset. 
    Correct outliers; Delete columns; Create new features for Human Footprint; Fill nan values; Scale and Offset climatic variables
    
    args:
        X: pd.Dataset
    returns:
        pd.Dataset
    """

    hf_cols = ['footprint_HumanFootprint-Built1994','footprint_HumanFootprint-Built2009',
        'footprint_HumanFootprint-croplands1992','footprint_HumanFootprint-croplands2005',
        'footprint_HumanFootprint-Lights1994',  'footprint_HumanFootprint-Lights2009',
        'footprint_HumanFootprint-NavWater1994', 'footprint_HumanFootprint-NavWater2009',
        'footprint_HumanFootprint-Pasture1993',  'footprint_HumanFootprint-Pasture2009',
        'footprint_HumanFootprint-Popdensity1990', 'footprint_HumanFootprint-Popdensity2010',
        'footprint_HumanFootprint-Railways', 'footprint_HumanFootprint-Roads',
        'footprint_HumanFootprint-HFP1993', 'footprint_HumanFootprint-HFP2009']

    ## Stardadization of data cleaning
    ## Create a safe copy
    Xx = X[hf_cols].copy()

    ## Replace Wrong Variables present in the HumanFootprint - See section HumanFootprint to understand the why these values are wrong
    ## Get all columns with value lesser than zero
    cols_less_zero = Xx.columns[(Xx<0).any(axis=0)]
    print(cols_less_zero)
    Xx = Xx.where(Xx > 0, 0.0, axis='index') #replace by zero

    ## Replace all values above a given threshold
    ## e.g HF Built should not be bigger than 100
    col_to_clean = ['footprint_HumanFootprint-Built1994','footprint_HumanFootprint-Built2009',
                    'footprint_HumanFootprint-croplands1992','footprint_HumanFootprint-croplands2005',
                    'footprint_HumanFootprint-Lights1994','footprint_HumanFootprint-Lights2009',
                    'footprint_HumanFootprint-Pasture1993','footprint_HumanFootprint-Pasture2009']
    Xx[col_to_clean] = Xx[col_to_clean].where(Xx[col_to_clean] <= 100, Xx[col_to_clean].median(), axis='index') ## replace by the median 

    ## Replace HPF by the median
    col_to_clean =['footprint_HumanFootprint-HFP1993', 'footprint_HumanFootprint-HFP2009']
    Xx[col_to_clean] = Xx[col_to_clean].where(Xx[col_to_clean]<51, Xx[col_to_clean].median(), axis='index')

    ## Replace a likely wrong value in Pop Density 
    ## It just transfer the value of pop density to the next/before pop density year - 97% percentile is 10.0, which means that value above are likely wrong
    col90 = ['footprint_HumanFootprint-Popdensity1990']
    col2010 = ['footprint_HumanFootprint-Popdensity2010']

    print(f" The 96% percentile of the Pop2010: {np.percentile(Xx[col2010],98)}")
    print(f" The 96% percentile of the Pop1990: {np.percentile(Xx[col90],98)}")
    print(f" Max value Pop2010: {Xx[col2010].max().values}")
    print(f" Max value Pop1990: {Xx[col90].max().values}")
    #Xx[col90] = Xx[col90].where(Xx[col90]< 11, Xx[col2010], axis='index')
    ## Replace values of Pop2010 with the same of Pop90
    Xx[col2010] = Xx[col2010].where(Xx[col2010]< 11, Xx[col90], axis='index')


    # ### CONSTRUCT NEW COLUMNS
    ## Find columns with year in the name of the column
    hf_cols_temp =[]
    for i in range(0,len(hf_cols)):
        find_number = re.findall(r'\d{4}$', hf_cols[i].split("-")[-1])
        #print(i,find_number)
        if find_number:
            hf_cols_temp.append(hf_cols[i])     
    print(f"Temporal columns: {hf_cols_temp}")  

    # ## Create new columns that contains the difference between two years
    new_columns=[]
    for i in range(0,len(hf_cols_temp),2):
        new_name = hf_cols_temp[i].split('-')[-1]
        name = re.findall(r'^(.+?)\d+$',new_name)[0]
        year0 = re.findall(r'\d{4}$', new_name)[0]
        year1 = re.findall(r'\d{4}$', hf_cols_temp[i+1])[0]
        full_name= f"{name}-{year0}-{year1}"
        new_columns.append(full_name)
        Xx[full_name] = Xx[hf_cols_temp[i+1]] - Xx[hf_cols_temp[i]]  

    print(f"Newly created columns: {new_columns}")


    ## Plot the distribution 
    # fig, ax = plt.subplots(figsize=(7,5))
    # ax = sns.boxplot(data=Xx)
    # plt.xticks(rotation=45, ha='right')
    # ax.set_title('Human Footprint - New Variables')
    # ax.set_xlabel('')  # Remove x-axis label if needed
    # plt.tight_layout()  # Prevents labels from being cut off
    # plt.show()


    ## It needs to replace all the variables RailWay bigger than 10 to 0. They are probably wrong
    Xx['footprint_HumanFootprint-Railways'] = Xx['footprint_HumanFootprint-Railways'].where(Xx['footprint_HumanFootprint-Railways']< 10, 0.0, axis='index')

    ## Clip Values of Cropland bigger than threshold and light lesser than threhold
    Xx['croplands-1992-2005'] = Xx['croplands-1992-2005'].clip(-10,20)
    Xx['Lights-1994-2009'] = Xx['Lights-1994-2009'].clip(-8,7)

    # #### -------------------------------------------------
    ## --------------------- CONCAT ---------------------------------------
    list_hf_railways_roads = [col for col in hf_cols if col not in hf_cols_temp]

    ## drop if they still exists:
    if Xx.columns.isin(hf_cols_temp).any()==True:
        Xx = Xx.drop(columns=hf_cols_temp, axis=1)

    ## Apply a clip value 
    X = pd.concat([X.drop(columns=hf_cols), Xx], axis=1)

    #### -----------------------------
    ### Prepare Climatic Variables 
    ### SCALE AND OFFSET 
    print('---'*30)
    print('Climatic Variables are being scaled and offset.')
    print()
    climatic_variables = ['average_Bio1', 'average_Bio2', 'average_Bio3', 'average_Bio4',
        'average_Bio5', 'average_Bio6', 'average_Bio7', 'average_Bio8',
        'average_Bio9', 'average_Bio10', 'average_Bio11', 'average_Bio12',
        'average_Bio13', 'average_Bio14', 'average_Bio15', 'average_Bio16',
        'average_Bio17', 'average_Bio18', 'average_Bio19']

    scale = 0.1
    offset_vector= (-273.15,0,0,0,-273.15,-273.15,0,-273.15,-273.15,-273.15,-273.15,0,0,0,0,0,0,0,0)

    X[climatic_variables] = X[climatic_variables]*0.1 + offset_vector


    ########### ------------------------------  FILL
    ## Deals with missing values
    print(f"----"*30)
    print(f"Filling Nan values.")


    cat_cols = ["landcover_LandCover"]
    not_cat_cols = [col for col in X.columns if col not in cat_cols]

    print(f"Missing values:{X.isna().sum().sum()}")

    ## fill na values with median
    X[not_cat_cols] = X[not_cat_cols].fillna(X.median(numeric_only=True))

    ### check for categorical columns, if yes, it will be filled them by mode
    X[cat_cols] = X[cat_cols].fillna(X[cat_cols].mode())

    print(f"Missing values after imputation:{X.isna().sum().sum()}")

    return X

