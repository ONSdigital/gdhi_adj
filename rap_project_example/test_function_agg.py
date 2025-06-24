def aggregate_data(df):
    # Calculate the missing proportion for each row
    df['missing_proportion'] = 100 - df['Proportion']
    
    return df
