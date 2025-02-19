import sys
import os

sys.path.insert(0, os.path.join(os.getcwd(),'../CIBUSmod'))

import pandas as pd
from CIBUSmod.impact.climate import _get_CO2eq_dict

class SAFAD_interface(object):

    def __init__(self, data_path):

        self.data_path = os.path.abspath(data_path)

        path_recipies = os.path.join(self.data_path,'SAFAD IP Recipes.csv')
        path_orig = os.path.join(self.data_path,'SAFAD IP Origin and Waste of RPC SE.csv')
        path_footprints = os.path.join(self.data_path,'SAFAD ID Footprints RPC.csv')
        path_transport_EF = os.path.join(self.data_path,'SAFAD IEF Transport.csv')

        self.raw_data = {
            'recipies' : None,
            'orig' : None,
            'footprints' : None,
            'transport_EF' : None
        }

        self.read_data('recipies',path_recipies)
        self.read_data('orig',path_orig)
        self.read_data('footprints',path_footprints)
        self.read_data('transport_EF',path_transport_EF)

    def read_data(self, table, path):

        if table == 'recipies':
            df = _read_SAFAD_csv(
                path = path,
                index = {'Food code':'code'}
            )
        elif table == 'orig':
            df = _read_SAFAD_csv(
                path = path,
                index = {'RPC Code':'code'}
            )
        elif table == 'footprints':
            df = _read_SAFAD_csv(
                path = path,
                index = {
                    'Long code':'code',
                    'Country code':'country code'
                }
            )
        elif table == 'transport_EF':
            df = _read_SAFAD_csv(
                path = path,
                index = {
                    'Importing, code':'import country code',
                    'Exported from, code':'country code'
                },
                cols = {
                    'Carbon dioxide':'Transport (CO2)',
                    'Methane, fossil':'Transport (CH4, fossil)',
                    'Nitrous oxide':'Transport (N2O)'
                }
            )

        self.raw_data.update({table : df})

    def get_footprints(self, code, use_waste_factor=False, importing_country='SE'):
        
        # Get data tables
        recipies, orig, footprints, transport_EF = self.raw_data.values()

        RPC_final = pd.Series(index=pd.Index([], name='code'), dtype=float)
        RPC = recipies.loc[[code]].set_index('Component code').rename_axis('code')
        
        while len(RPC) > 0:
            if isinstance(RPC, pd.Series):
                RPC = pd.merge(RPC, recipies.loc[RPC.index], left_index=True, right_index=True).set_index('Component code').rename_axis('code')
            if 'factor' in RPC.columns:
                RPC = (RPC['factor'] * (RPC['Percentage']/100) * RPC['Reverse Yield Factor'] * RPC['Allocation Factor']).rename('factor')
            else:
                RPC = ((RPC['Percentage']/100) * RPC['Reverse Yield Factor'] * RPC['Allocation Factor']).rename('factor')
            
            RPC_to_final = RPC.loc[[code in orig.index for code in RPC.index]]
            RPC = RPC.loc[[code not in orig.index for code in RPC.index]]
            RPC_final = pd.concat([RPC_final, RPC_to_final]).rename('factor')
        
        RPC_orig = (
            pd.merge(RPC_final, orig, left_index=True, right_index=True)
            .set_index('Producer Country Code', append=True)
            .rename_axis(index=['code', 'country code'])
        )
        
        # Drop country code "SE" (Sweden) as we are only interested in imports
        RPC_orig = RPC_orig.drop(importing_country, level='country code', errors='ignore')
        # Calculate import sahres
        RPC_orig['Import Share'] = RPC_orig['Share'].groupby('code').apply(lambda x: x/x.sum()).droplevel(0)
        
        RPC_footprint = pd.merge(RPC_orig, footprints, left_index=True, right_index=True)

        # Add in transport emissions
        RPC_footprint = pd.merge(RPC_footprint, transport_EF.loc[importing_country], left_index=True, right_index=True)
        
        RPC_footprint['Import Share w. data'] = RPC_footprint['Import Share'].groupby('code').apply(lambda x: x/x.sum()).droplevel(0)

        if (RPC_footprint['Import Share'].groupby('code').sum() != 1).any():
            pass
            # Handle missing data for some import countries

        # Add country as index
        RPC_footprint.set_index('Producer Country Name', append=True, inplace=True)
        
        final_footprint = (
            RPC_footprint
            .loc[:,'Carbon footprint, primary production':'Transport (N2O)']
            .mul((
                    RPC_footprint['factor'] *
                    ((1/(1 - RPC_footprint['Waste'])) if use_waste_factor else 1) *
                    RPC_footprint['Import Share w. data']
                ),
                axis=0
            )
            .groupby('Producer Country Name').sum().unstack().rename_axis(['footprint','country'])
        )
    
        return final_footprint

    def get_GHG(
        self,
        code,
        CO2eq = 'GWP100 AR4'
        ):
        
        footprints_to_process_and_GHG = {
            'Mineral fertiliser production (CO2)' : ('Mineral fertiliser production', 'CO2'),
            'Capital goods (CO2)' : ('Capital goods', 'CO2'),
            'Energy primary production (CO2)' : ('Energy primary production', 'CO2'),
            'Land use change (CO2)' : ('Land use change', 'CO2'),
            'Transport (CO2)' : ('Transport', 'CO2'),
            'Mineral fertiliser production (CH4, fossil)' : ('Mineral fertiliser production', 'CH4fos'),
            'Capital goods (CH4, fossil)' : ('Capital goods', 'CH4fos'),
            'Energy primary production (CH4, fossil)' : ('Energy primary production', 'CH4fos'),
            'Transport (CH4, fossil)' : ('Transport', 'CH4fos'),
            'Soil emissions (CH4, biogenic)' : ('Soil emissions', 'CH4bio'),
            'Enteric fermentation (CH4, biogenic)' : ('Enteric fermentation', 'CH4bio'),
            'Manure management (CH4, biogenic)' : ('Manure management', 'CH4bio'),
            'Mineral fertiliser production (N2O)' : ('Mineral fertiliser production', 'N2O'),
            'Capital goods (N2O)' : ('Capital goods', 'N2O'),
            'Soil emissions (N2O)' : ('Soil emissions', 'N2O'),
            'Energy primary production (N2O)' : ('Energy primary production', 'N2O'),
            'Manure management (N2O)' : ('Manure management', 'N2O'),
            'Transport (N2O)' : ('Transport', 'N2O')
        }

        GHG_emissions = self.get_footprints(code).loc[footprints_to_process_and_GHG.keys()]
        GHG_emissions.index = pd.MultiIndex.from_tuples(
            [footprints_to_process_and_GHG[f]+(c,) for f,c in GHG_emissions.index],
            names = ['process', 'compound', 'country']
        )

        if CO2eq:
            to_CO2eq = _get_CO2eq_dict(CO2eq)

            GHG_emissions = GHG_emissions * [to_CO2eq[cp] for cp in GHG_emissions.index.get_level_values('compound')]

        return GHG_emissions

def _read_SAFAD_csv(path, index:dict, cols:None|dict=None):
    df = pd.read_csv(path, index_col=list(index.keys())).rename_axis(list(index.values()))
    if len(index) > 1:
        df.index = df.index.set_levels([level.str.strip() for level in df.index.levels])
    else:
        df.index = df.index.str.strip()
    if cols:
        df = df.loc[:,list(cols.keys())].rename(cols, axis=1)
    return df