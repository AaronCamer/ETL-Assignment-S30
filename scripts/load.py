import pandas as pd
from scripts.transform import Transform

import pandas as pd

class Load:
    def __init__(self, csv_dir) -> None:
        self.csv_dir = csv_dir

    def load_data(self, final_data, file_name):
        # Export to CSV
        try:
            final_data.to_csv(self.csv_dir / file_name, index=False, sep=';')
            print(f'Successfully created {file_name}')
        except:
            raise Exception