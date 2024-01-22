
def print_dataframe(df_buffer, spark):
    df_buffer \
        .write \
        .format("console") \
        .save()
