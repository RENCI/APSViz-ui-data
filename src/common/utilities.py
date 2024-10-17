'''
MIT License

Copyright (c) 2022, 2023, 2024 Renaissance Computing Institute

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

import sys 
import numpy as np
import pandas as pd
import re
import xarray as xr
from scipy import spatial as sp
from datetime import date, datetime
import time as tm

# load the logger class
from src.common.logger import LoggingUtil

# get the log level and directory from the environment (or default).
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("geopoints_url", level=log_level, line_format='long', log_file_path=log_path)

#logger.debug("utilities:Xarray Version: %s', xr.__version__)
Kmax=10
got_kdtree=None
TOL=10e-5
debug=True # False

# Specify available reanalysis years
Ymin=1979
Ymax=2023
YEARS=[item for item in range(Ymin, Ymax+1)]
#logger.debug('utilities:Ymin, Ymax: %s, %s', Ymin,Ymax)

fileext='.d0.no-unlim.T.rc.nc'
#fileext='.d4.no-unlim.T.rc.nc';
#logger.debug('utilities:fileext: %s', fileext)

# Default standard location is on the primary RENCI TDS
#urldirformat="http://tds.renci.org/thredds/dodsC/Reanalysis/ADCIRC/ERA5/hsofs/%d-post"
urldirformat="https://tdsres.apps.renci.org/thredds/dodsC/ReanalysisV2/ADCIRC/ERA5/hsofs.V2/%d-post"
#urldirformat="http://tds.renci.org/thredds/dodsC/Reanalysis/ADCIRC/ERA5/ec95d/%d"

def get_adcirc_grid_from_ds(ds):
    """
    """
    agdict = {}
    agdict['lon'] = ds['x'][:]
    agdict['lat'] = ds['y'][:]
    agdict['ele'] = ds['element'][:,:] - 1
    agdict['depth'] = ds['depth'][:]
    agdict['latmin'] = np.mean(ds['y'][:])  # needed for scaling lon/lat plots
    return agdict

def attach_element_areas(agdict):
    """
    """
    x=agdict['lon'].values
    y=agdict['lat'].values
    e=agdict['ele'].values
    
    # COMPUTE GLOBAL DX,DY, Len, angles
    i1=e[:,0]
    i2=e[:,1]
    i3=e[:,2]

    x1=x[i1];x2=x[i2];x3=x[i3];
    y1=y[i1];y2=y[i2];y3=y[i3];

    # coordinate deltas
    dx23=x2-x3
    dx31=x3-x1
    dx12=x1-x2
    dy23=y2-y3
    dy31=y3-y1
    dy12=y1-y2

    # lengths of sides
    a = np.sqrt(dx12*dx12 + dy12*dy12)
    b = np.sqrt(dx31*dx31 + dy31*dy31)
    c = np.sqrt(dx23*dx23 + dy23*dy23)
    
    agdict['areas'] = ( x1*dy23 + x2*dy31 + x3*dy12 )/2.
    agdict['edge_lengths']=[a, b, c]
    agdict['dl']=np.mean(agdict['edge_lengths'],axis=0)

    return agdict
    
def basis2d_withinElement(phi):
    """
    """
    interior_status = np.all(phi[:]<=1+TOL,axis=1) & np.all(phi[:]>=0-TOL,axis=1)
    return interior_status

def basis2d(agdict,xylist,j):
    """
    """
    # check length of j and xylist
    # check for needed arrays in agdict
    phi=[]
    #nodes for the elements in j
    n3=agdict['ele'][j]
    x=agdict['lon'][n3].values     
    x1=x[:,0];x2=x[:,1];x3=x[:,2];
    y=agdict['lat'][n3].values
    y1=y[:,0];y2=y[:,1];y3=y[:,2];  
    areaj=agdict['areas'][j]
    xp=xylist[:,0]
    yp=xylist[:,1]
    # Basis function 1
    a=(x2*y3-x3*y2)
    b=(y2-y3)
    c=-(x2-x3)
    phi0=(a+b*xp+c*yp)/(2.0*areaj)
    # Basis function 2
    a=(x3*y1-x1*y3)
    b=(y3-y1)
    c=-(x3-x1)
    phi1=(a+b*xp+c*yp)/(2.0*areaj)
    # Basis function 3
    a=(x1*y2-x2*y1)
    b=(y1-y2)
    c=-(x1-x2)
    phi2=(a+b*xp+c*yp)/(2.0*areaj)
    return np.array([phi0, phi1, phi2]).T

def get_adcirc_time_from_ds(ds):
    """
    """
    return {'time': ds['time']}

def f63_to_xr(url):
    """
    """
    dropvars=['neta', 'nvel',  'max_nvdll', 'max_nvell']
    return xr.open_dataset(url,drop_variables=dropvars)

def get_adcirc_slice_from_ds(ds,v,it=0):
    """
    """
    advardict = {}
    var = ds.variables[v]
    if re.search('max', v) or re.search('depth', v):
        var_d = var[:] # the actual data
    else:
        if ds.variables[v].dims[0] == 'node':
            #logger.debug('ds: transposed data found')
            var_d = var[it,:].T # the actual data
        elif ds.variables[v].dims[0] == 'time':
            var_d = var[:,it] # the actual data
        else:
            logger.debug('Unexpected leading variable name: %s. Abort', ds.variables[v].dims)
            sys.exit(1)
    #var_d[var_d.mask] = np.nan
    advardict['var'] = var_d.data
    return advardict

def ComputeTree(agdict):
    """
    Given lon,lat,ele in agdict,compute element centroids and 
    generate the ADCIRC grid KDTree
    returns agdict with tree
    """

    global got_kdtree # Try not to if already done
    t0=tm.time()
    try:
        x=agdict['lon'].values.ravel() # ravel; not needed
        y=agdict['lat'].values.ravel()
        e=agdict['ele'].values
    except Exception as e:
        logger.debug('Did not find lon,lat,ele data in agdict.')
        sys.exit(1)
    xe=np.mean(x[e],axis=1)
    ye=np.mean(y[e],axis=1)
    if got_kdtree is None: # Still want to build up the data for agdict, we just do not need the tree reevaluated for every year
        agdict['tree']=tree = sp.KDTree(np.c_[xe,ye])
        got_kdtree=tree
    else:
        agdict['tree']=got_kdtree
    logger.debug('Build annual KDTree time is: %s seconds', tm.time()-t0)
    return agdict

def ComputeQuery(xylist, agdict, kmax=10):
    """
    Generate the kmax-set of nearest neighbors to each lon,lat pair in xylist.
    Each test point (each lon/lat pair) gets associated distance (dd) and element (j) objects 
    At this stage it is possible that some test points are not interior to the nearest element. We will
    subsequently check that.

    dd: num points by neighbors
    j: num points by neighbors
    """
    t0=tm.time()
    agresults=dict()
    dd, j = agdict['tree'].query(xylist, k=kmax)
    if kmax==1:
        dd=dd.reshape(-1,1)
        j=j.reshape(-1,1)
    agresults['distance']=dd
    agresults['elements']=j
    agresults['number_neighbors']=kmax
    agresults['geopoints']=xylist # We shall use this later
    logger.debug('KDTree query of size: %s took: %s seconds', kmax, tm.time()-t0)
    return agresults

def ComputeBasisRepresentation(xylist, agdict, agresults):
    """
    For each test point with kmax number_neighbors, compute linear basis for
    each neighbor. Then, check which, if any, element the test point actually resides within.
    If none, then the returned basis functions (i.e., interpolation weights) are set to nans. 

    If an input point is an "exact" grid point (i.e., ADCIRC grid node), then ambiguity
    may arise regarding the best element and multiple True statuses can occur. Here we 
    also keep the nearest element value. We do this by reverse iterating in the zip function
    """

    # First build all the basis weights and determine if it was interior or not
    t0=tm.time()
    kmax = agresults['number_neighbors']
    j = agresults['elements']
    phival_list=list()
    within_interior=list()
    for k_value in range(0,kmax):
        phival=basis2d(agdict,xylist,j[:,k_value])
        phival_list.append(phival)
        within_interior.append(basis2d_withinElement(phival))
    
    #detailed_weights_elements(phival_list, j)

    # Second only retain the "interior" results or nans if none
    final_weights= np.full( (phival_list[0].shape[0],phival_list[0].shape[1]),np.nan)
    final_jvals = np.full( j.T[0].shape[0],-99999)
    final_status = np.full( within_interior[0].shape[0],False)
    # Loop backwards. thus keeping the "nearest" True for each geopoints for each k in kmax
    for pvals,jvals,testvals in zip(phival_list[::-1], j.T[::-1], within_interior[::-1]):  # THis loops over Kmax values
        final_weights[testvals] = pvals[testvals]
        final_jvals[testvals]=jvals[testvals]
        final_status[testvals] = testvals[testvals]

    agresults['final_weights']=final_weights
    agresults['final_jvals']=final_jvals
    agresults['final_status']=final_status
    logger.debug('Compute of basis took: %s seconds', tm.time()-t0)
    # Keep the list if the user needs to know after the fact
    outside_elements = np.argwhere(np.isnan(final_weights).all(axis=1)).ravel()
    agresults['outside_elements']=outside_elements
    return agresults

def detailed_weights_elements(phival_list, j):
    """
    This is only used for understanding better the detailed behavior of a particular grid
    It is not invoked for general use
    """
    for pvals,jvals in zip(phival_list,j.T):
        df_pvals = pd.DataFrame(pvals, columns=['Phi0','Phi1','Phi2'])
        df_pvals.index = df_pvals.index
        df_jvals = pd.DataFrame(jvals+1,columns=['Element+1'])
        df = pd.concat([df_pvals,df_jvals],axis=1)
        df.index = df.index+1
        df.index = df.index.astype(int)
        logger.debug('Dataframe: %s', df.loc[2].to_frame().T)

def WaterLevelReductions(t, data_list, final_weights):
    """
    Each data_list is a df for a single point containing 3 columns, one for 
    each node in the containing element. 
    These columns are reduced using the final_weights previously calculated
    
    A final df is returned with index=time and a single column for each of the
    input test points (some of which may be partially or completely nan)
    """
    try:
        final_list = list()
        for index,dataseries,weights in zip(range(0,len(data_list)), data_list,final_weights):
            reduced_data = np.matmul(dataseries.values, weights.T)
            df = pd.DataFrame(reduced_data, index=t, columns=[f'P{index+1}'])
            final_list.append(df)
        df_final_data = pd.concat(final_list, axis=1)
    except Exception as e:
        df_final_data=None
    return df_final_data

def GenerateMetadata(agresults):
    """
    Here we want to simply assist the user by reporting back the lon/lat values for each geopoint.
    This should be the same as the input dataset. -99999 indicates an element was not found in the grid.
    """
    
    df_lonlat=pd.DataFrame(agresults['geopoints'], columns=['LON','LAT'])
    df_elements = pd.DataFrame(agresults['final_jvals']+1, columns=['Element (1-based)'])
    df_elements.replace(-99998,-99999,inplace=True)
    df_meta=pd.concat( [df_lonlat,df_elements], axis=1)
    df_meta['Point']=df_meta.index+1
    df_meta.set_index('Point', inplace=True)
    df_meta.rename('P{}'.format, inplace=True)
    
    return df_meta

def ConstructReducedWaterLevelData_from_ds(ds, agdict, agresults, variable_name=None): 
    """
    This method acquires ADCIRC water levels for the list of geopoints/elements. 
    For each specified point in the grid, the resulting time series' are reduced to a single time series using 
    a (basis2d) weighted sum. For a non-nan value to result in the final data, the product data must:
    1) Be non-nan for each time series at the specified time tick
    2) The test point must be interior to the specified element
    """
    
    if variable_name is None:
        logger.error('User MUST supply the correct variable name')
        sys.exit(1)
    logger.debug('Variable name is: %s', variable_name)
    t0 = tm.time()
    data_list=list()
    final_weights = agresults['final_weights']
    final_jvals = agresults['final_jvals']

    acdict=get_adcirc_time_from_ds(ds)
    t=acdict['time'].values
    e = agdict['ele'].values

    logger.debug('Before removal of out-of-triangle jvals: %s', final_jvals.shape)
    mask =(final_jvals == -99999)
    final_jvals=final_jvals[~mask]
    logger.debug(f'After removal of out-of-triangle jvals: %s', final_jvals.shape)
    
    for vstation in final_jvals:
        advardict = get_adcirc_slice_from_ds(ds,variable_name,it=e[vstation])
        df = pd.DataFrame(advardict['var'])
        data_list.append(df)
    logger.debug('Time to fetch annual all test station (triplets) was: %s seconds', tm.time()-t0)
    df_final=WaterLevelReductions(t, data_list, final_weights)
    t0=tm.time()
    df_meta=GenerateMetadata(agresults) # This is here mostly for future considerations
    logger.debug('Time to reduce annual: %s, test stations is: %s seconds', len(final_jvals), tm.time()-t0)
    agresults['final_reduced_data']=df_final
    agresults['final_meta_data']=df_meta
    
    return agresults

# NOTE We do not need to rebuild the tree for each year since the grid is unchanged.
def Combined_pipeline(url, variable_name, lon, lat, nearest_neighbors=10):
    """
    Interpolate for one year. 
    
    df_excluded_geopoints lists only those stations excluded by element tests. 
    Some could be all nans due to dry points

    No flanks removed in this method as the caller may want to see everything
    """
    t0=tm.time()
    geopoints=np.array([[lon,lat]])
    ds = f63_to_xr(url)
    agdict=get_adcirc_grid_from_ds(ds)
    agdict=attach_element_areas(agdict)
    logger.debug('Compute_pipeline initiation: %s seconds', tm.time()-t0)

    logger.debug('Start annual KDTree pipeline LON: %s LAT: %s', geopoints[0][0], geopoints[0][1])
    agdict=ComputeTree(agdict)
    agresults=ComputeQuery(geopoints, agdict, kmax=nearest_neighbors)
    agresults=ComputeBasisRepresentation(geopoints, agdict, agresults)
    agresults=ConstructReducedWaterLevelData_from_ds(ds, agdict, agresults, variable_name=variable_name)

    logger.debug('Basis function Tolerance value is: %s', TOL)
    logger.debug('List of %s stations not assigned to any grid element follows for kmax: %s', len(agresults["outside_elements"]), nearest_neighbors)
    t0=tm.time()
    df_product_data=agresults['final_reduced_data']
    df_product_metadata=agresults['final_meta_data']
    df_excluded_geopoints=pd.DataFrame(geopoints[agresults['outside_elements']], index=agresults['outside_elements']+1, columns=['lon','lat'])
    logger.debug('Compute_pipeline cleanup: %s seconds', tm.time()-t0)
    logger.debug('Finished annual Combined_pipeline')
    
    return df_product_data, df_product_metadata, df_excluded_geopoints
