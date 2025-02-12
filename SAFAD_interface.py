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

        SAFAD_recipies = pd.read_csv(path_recipies, index_col='Food code').rename_axis('code')
        SAFAD_recipies.index = SAFAD_recipies.index.str.strip()

        SAFAD_orig = pd.read_csv(path_orig,index_col='RPC Code').rename_axis('code')
        SAFAD_orig.index = SAFAD_orig.index.str.strip()
        
        SAFAD_footprints = pd.read_csv(path_footprints, index_col=['Long code', 'Country code']).rename_axis(index=['code','country code'])
        SAFAD_footprints.index = SAFAD_footprints.index.set_levels([level.str.strip() for level in SAFAD_footprints.index.levels])

        self.raw_data = {
            'recipies' : SAFAD_recipies,
            'orig' : SAFAD_orig,
            'footprints' : SAFAD_footprints
        }

    def get_footprints(self, code):
        
        # Get data tables
        SAFAD_recipies, SAFAD_orig, SAFAD_footprints = self.raw_data.values()

        RPC_final = pd.Series(index=pd.Index([], name='code'), dtype=float)
        RPC = SAFAD_recipies.loc[[code]].set_index('Component code').rename_axis('code')
        
        while len(RPC) > 0:
            if isinstance(RPC, pd.Series):
                RPC = pd.merge(RPC, SAFAD_recipies.loc[RPC.index], left_index=True, right_index=True).set_index('Component code').rename_axis('code')
            if 'factor' in RPC.columns:
                RPC = (RPC['factor'] * (RPC['Percentage']/100) * RPC['Reverse Yield Factor'] * RPC['Allocation Factor']).rename('factor')
            else:
                RPC = ((RPC['Percentage']/100) * RPC['Reverse Yield Factor'] * RPC['Allocation Factor']).rename('factor')
            
            RPC_to_final = RPC.loc[[code in SAFAD_orig.index for code in RPC.index]]
            RPC = RPC.loc[[code not in SAFAD_orig.index for code in RPC.index]]
            RPC_final = pd.concat([RPC_final, RPC_to_final]).rename('factor')
        
        RPC_orig = (
            pd.merge(RPC_final, SAFAD_orig, left_index=True, right_index=True)
            .set_index('Producer Country Code', append=True)
            .rename_axis(index=['code', 'country code'])
        )
        
        # Drop country code "SE" (Sweden) as we are only interested in imports
        RPC_orig = RPC_orig.drop('SE', level='country code', errors='ignore')
        # Calculate import sahres
        RPC_orig['Import Share'] = RPC_orig['Share'].groupby('code').apply(lambda x: x/x.sum()).droplevel(0)
        
        RPC_footprint = pd.merge(RPC_orig, SAFAD_footprints, left_index=True, right_index=True)
        
        RPC_footprint['Import Share w. data'] = RPC_footprint['Import Share'].groupby('code').apply(lambda x: x/x.sum()).droplevel(0)

        if (RPC_footprint['Import Share'].groupby('code').sum() != 1).any():
            pass
            # Handle missing data for some import countries
        
        final_footprint = (
            RPC_footprint
            .loc[:,'Carbon footprint, primary production':'Manure management (N2O)']
            .mul(
                (RPC_footprint['factor'] * (1/(1 - RPC_footprint['Waste'])) * RPC_footprint['Import Share w. data']),
                axis=0
            )
            .sum()
        )
    
        return final_footprint

    def get_GHG(
        self,
        code,
        processes = ['Mineral fertiliser production',
                    'Capital goods',
                    'Energy primary production',
                    'Land use change',
                    'Enteric fermentation',
                    'Manure management'],
        CO2eq = 'GWP100 AR4'
        ):

        # THERE SEEM TO BE SOME ERRORS IN THE DATABASE
        # SO THIS DOES NOT PRODUCE RELIABLE RESULTS!!!

        
        SAFAD_footprints_to_process_and_GHG = {
            'Mineral fertiliser production (CO2)' : ('Mineral fertiliser production', 'CO2'),
            'Capital goods (CO2)' : ('Capital goods', 'CO2'),
            'Energy primary production (CO2)' : ('Energy primary production', 'CO2'),
            'Land use change (CO2)' : ('Land use change', 'CO2'),
            'Mineral fertiliser production (CH4, fossil)' : ('Mineral fertiliser production', 'CH4fos'),
            'Capital goods (CH4, fossil)' : ('Capital goods', 'CH4fos'),
            'Energy primary production (CH4, fossil)' : ('Energy primary production', 'CH4fos'),
            'Soil emissions (CH4, biogenic)' : ('Soil emissions', 'CH4bio'),
            'Enteric fermentation (CH4, biogenic)' : ('Enteric fermentation', 'CH4bio'),
            'Manure management (CH4, biogenic)' : ('Manure management', 'CH4bio'),
            'Mineral fertiliser production (N2O)' : ('Mineral fertiliser production', 'N2O'),
            'Capital goods (N2O)' : ('Capital goods', 'N2O'),
            'Soil emissions (N2O)' : ('Soil emissions', 'N2O'),
            'Energy primary production (N2O)' : ('Energy primary production', 'N2O'),
            'Manure management (N2O)' : ('Manure management', 'N2O')
        }

        GHG_emissions = self.get_footprints(code).loc[SAFAD_footprints_to_process_and_GHG.keys()]
        GHG_emissions.index = pd.MultiIndex.from_tuples(
            [SAFAD_footprints_to_process_and_GHG[i] for i in GHG_emissions.index],
            names = ['process', 'compound']
        )
        GHG_emissions = GHG_emissions.loc[processes].groupby('compound').sum()

        if CO2eq:
            to_CO2eq = _get_CO2eq_dict(CO2eq)

            GHG_emissions = GHG_emissions * [to_CO2eq[cp] for cp in GHG_emissions.index]

        return GHG_emissions

    def get_CO2e(
        self,
        code,
        processes = ['Mineral fertiliser production',
                    'Capital goods',
                    'Energy primary production',
                    'Land use change',
                    'Enteric fermentation',
                    'Manure management']
    ):
        
        SAFAD_footprints_to_process_CO2e = {
            'Mineral fertiliser production (CO2e)' : 'Mineral fertiliser production',
            'Capital goods (CO2e)' : 'Capital goods',
            'Energy primary production (CO2e)' : 'Energy primary production',
            'Land use change (CO2e)' : 'Land use change',
            'Soil emissions (CO2e)' : 'Soil emissions',
            'Enteric fermentation (CO2e)' : 'Enteric fermentation',
            'Manure management (CO2e)' : 'Manure management',
        }
        
        GHG_emissions = self.get_footprints(code).loc[SAFAD_footprints_to_process_CO2e.keys()]
        GHG_emissions.index = pd.Index(
            [SAFAD_footprints_to_process_CO2e[i] for i in GHG_emissions.index],
            name = 'process'
        )
        GHG_emissions = GHG_emissions.loc[processes].sum()

        return GHG_emissions