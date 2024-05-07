# mcr-py

## Installation

Recommendation: Use mamba to create the environment faster.

```
conda config --add channels conda-forge
conda config --set channel_priority strict
conda install mamba
```

Setup environment:

```
mamba env create -f environment.yaml
conda activate mcr-py
```

### Running Analysis for Cologne

Alternative way for first part (GTFS stuff):
- run data_pipeline.ipynb, it automatically does the rest of gtfs calculations

Argument 777 stands for the gtfs feed available for cologne. Check for other versions using `gtfs list --country-code DE`
To include multiple operators available (e.g. also include S-Bahn from db), recheck how the file is used.
```
python src/main.py gtfs download 777 ./data/vrs.zip
```
change to another city, defining the boundary for the respective city
- Run `area.ipynb`

Crops the provided gtfs file, excluding all stops that are outside the provided boundary and pruning the trips based on this.
Then prunes based on the provided time-start and time-end, in the same manner.

Clean deals with pecularities of the gtfs format, like diverging paths on the 'same' route, circular trips (which are removed) and finally
converts and reduces the data to the data used by the algorithm.
Build-structures prepares the format exactly for the algorithm.
```
python src/main.py gtfs crop ./data/vrs.zip ./data/cologne_gtfs.zip \                                                                                        ─╯
    --geometa-path ./data/geometa.pkl \
    --time-start 23.06.2023-00:00:00 \
    --time-end 24.06.2023-00:00:00 
python src/main.py gtfs clean  ./data/cologne_gtfs.zip ./data/cleaned/
python src/main.py build-structures ./data/cleaned/ ./data/structs.pkl
```

Need to download the Copernicus Land Cover data from the EU

- Run `landuse.ipynb`
- Run `bicycle_locations.ipynb`
- Run `mcr5_input.ipynb`

- Run `mcr5.ipynb`

# Results
- Run `mcr5_results_calculation.ipynb`

- See `mcr5_results.ipynb`
