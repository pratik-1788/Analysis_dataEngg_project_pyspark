class WriteDfIntoLocal:

    def __init__(self, mode, data_formated):
        self.mode = mode
        self.data_formated = data_formated

    def write(self, df, path):
        df.write \
          .format(self.data_formated) \
          .option("header", "true") \
          .mode(self.mode) \
          .save(path)