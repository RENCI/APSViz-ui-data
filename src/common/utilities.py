# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Time series extraction

    Authors: Jeffrey L. Tilson, Brian O. Blanton 8/2024
"""
import sys
import numpy as np
import pandas as pd
import re
import xarray as xr
import time as tm
from scipy import spatial as sp
from src.common.logger import LoggingUtil
import datetime as dt


class GeoUtilities:
    """
    Class that has a number of static methods used throughout this component

    """
    def __init__(self, _app_name='GeoUtilities.TEST',  _logger=None):
        """
        inits the class

        :param _app_name:
        :param _logger:
        """
        # if a reference to a logger was passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging(_app_name, level=log_level, line_format='medium', log_file_path=log_path)

        self.Kmax = 10
        self.got_kdtree = None
        self.TOL = 10e-5
        self.debug = True  # False

        # Specify available reanalysis years
        self.Ymin = 1979
        self.Ymax = 2023
        self.YEARS = [item for item in range(self.Ymin, self.Ymax + 1)]
        # logger.debug('utilities:Ymin, Ymax: %s, %s', Ymin,Ymax)

        self.fileext = '.d0.no-unlim.T.rc.nc'
        # self.fileext='.d4.no-unlim.T.rc.nc';
        # self.logger.debug('utilities:fileext: %s', fileext)

        # Default standard location is on the primary RENCI TDS
        # self.urldirformat="https://tds.renci.org/thredds/dodsC/Reanalysis/ADCIRC/ERA5/hsofs/%d-post"
        # self.urldirformat="https://tds.renci.org/thredds/dodsC/Reanalysis/ADCIRC/ERA5/ec95d/%d"
        self.urldirformat = "https://tdsres.apps.renci.org/thredds/dodsC/ReanalysisV2/ADCIRC/ERA5/hsofs.V2/%d-post"

        self.keep_hours = [0, 6, 12, 18]

    @staticmethod
    def get_adcirc_grid_from_ds(ds):
        """

        """
        agdict: dict = {'lon': ds['x'][:], 'lat': ds['y'][:], 'ele': ds['element'][:, :] - 1, 'depth': ds['depth'][:], 'latmin': np.mean(ds['y'][:])}

        return agdict

    @staticmethod
    def attach_element_areas(agdict):
        """
        """
        x = agdict['lon'].values
        y = agdict['lat'].values
        e = agdict['ele'].values

        # COMPUTE GLOBAL DX,DY, Len, angles
        i1 = e[:, 0]
        i2 = e[:, 1]
        i3 = e[:, 2]

        x1 = x[i1]
        x2 = x[i2]
        x3 = x[i3]

        y1 = y[i1]
        y2 = y[i2]
        y3 = y[i3]

        # coordinate deltas
        dx23 = x2 - x3
        dx31 = x3 - x1
        dx12 = x1 - x2
        dy23 = y2 - y3
        dy31 = y3 - y1
        dy12 = y1 - y2

        # lengths of sides
        a = np.sqrt(dx12 * dx12 + dy12 * dy12)
        b = np.sqrt(dx31 * dx31 + dy31 * dy31)
        c = np.sqrt(dx23 * dx23 + dy23 * dy23)

        agdict['areas'] = (x1 * dy23 + x2 * dy31 + x3 * dy12) / 2.
        agdict['edge_lengths'] = [a, b, c]
        agdict['dl'] = np.mean(agdict['edge_lengths'], axis=0)

        return agdict

    def basis2d_withinElement(self, phi):
        """
        """
        interior_status = np.all(phi[:] <= 1 + self.TOL, axis=1) & np.all(phi[:] >= 0 - self.TOL, axis=1)

        return interior_status

    @staticmethod
    def basis2d(agdict, xylist, j):
        """
        """
        # check length of j and xylist
        # check for the necessary arrays in agdict
        phi = []

        # nodes for the elements in j
        n3 = agdict['ele'][j]

        x = agdict['lon'][n3].values
        x1 = x[:, 0]
        x2 = x[:, 1]
        x3 = x[:, 2]

        y = agdict['lat'][n3].values
        y1 = y[:, 0]
        y2 = y[:, 1]
        y3 = y[:, 2]

        areaj = agdict['areas'][j]
        xp = xylist[:, 0]
        yp = xylist[:, 1]

        # Basis function 1
        a = (x2 * y3 - x3 * y2)
        b = (y2 - y3)
        c = -(x2 - x3)
        phi0 = (a + b * xp + c * yp) / (2.0 * areaj)

        # Basis function 2
        a = (x3 * y1 - x1 * y3)
        b = (y3 - y1)
        c = -(x3 - x1)
        phi1 = (a + b * xp + c * yp) / (2.0 * areaj)

        # Basis function 3
        a = (x1 * y2 - x2 * y1)
        b = (y1 - y2)
        c = -(x1 - x2)
        phi2 = (a + b * xp + c * yp) / (2.0 * areaj)

        return np.array([phi0, phi1, phi2]).T

    @staticmethod
    def get_adcirc_time_from_ds(ds):
        """
        """
        return {'time': ds['time']}

    @staticmethod
    def f63_to_xr(url):
        """
        """
        dropvars = ['neta', 'nvel', 'max_nvdll', 'max_nvell']

        return xr.open_dataset(url, drop_variables=dropvars)

    @staticmethod
    def get_adcirc_slice_from_ds(ds, v, it=0):
        """
        """
        advardict = {}

        var = ds.variables[v]

        if re.search('max', v) or re.search('depth', v):
            var_d = var[:]  # the actual data
        else:
            if ds.variables[v].dims[0] == 'node':
                # self.logger.debug('ds: transposed data found')
                var_d = var[it, :].T  # the actual data
            elif ds.variables[v].dims[0] == 'time':
                var_d = var[:, it]  # the actual data
            else:
                raise f'Unexpected leading variable name: {ds.variables[v].dims}. Abort'

        # var_d[var_d.mask] = np.nan
        advardict['var'] = var_d.data

        return advardict

    def ComputeTree(self, agdict):
        """
        Given lon,lat,ele in agdict,compute element centroids and
        generate the ADCIRC grid KDTree
        returns agdict with tree
        """

        t0 = tm.time()

        try:
            x = agdict['lon'].values.ravel()  # ravel; not needed
            y = agdict['lat'].values.ravel()
            e = agdict['ele'].values
        except Exception as e:
            self.logger.debug('Did not find lon,lat,ele data in agdict.')
            sys.exit(1)

        xe = np.mean(x[e], axis=1)
        ye = np.mean(y[e], axis=1)

        # Still want to build up the data for agdict, we just do not need the tree reevaluated for every year
        if self.got_kdtree is None:
            agdict['tree'] = tree = sp.KDTree(np.c_[xe, ye])
            self.got_kdtree = tree
        else:
            agdict['tree'] = self.got_kdtree

        self.logger.debug('Build annual KDTree time is: %s seconds', tm.time() - t0)
        return agdict

    def ComputeQuery(self, xylist, agdict, kmax=10):
        """
        Generate the kmax-set of nearest neighbors to each lon,lat pair in xylist.
        Each test point (each lon/lat pair) gets associated distance (dd) and element (j) objects
        At this stage it is possible that some test points are not interior to the nearest element.
        We will subsequently check that.

        dd: num points by neighbors
        j: num points by neighbors
        """
        t0 = tm.time()
        agresults = dict()

        dd, j = agdict['tree'].query(xylist, k=kmax)

        if kmax == 1:
            dd = dd.reshape(-1, 1)
            j = j.reshape(-1, 1)

        agresults['distance'] = dd
        agresults['elements'] = j
        agresults['number_neighbors'] = kmax
        agresults['geopoints'] = xylist  # We shall use this later

        self.logger.debug('KDTree query of size: %s took: %s seconds', kmax, tm.time() - t0)

        return agresults

    def ComputeBasisRepresentation(self, xylist, agdict, agresults):
        """
        For each test point with kmax number_neighbors, compute linear basis for
        each neighbor.

        Then, check which, if any, element the test point actually resides within.
        If none, then the returned basis functions (i.e., interpolation weights) are set to nans.

        If an input point is an "exact" grid point (i.e., ADCIRC grid node), then ambiguity
        may arise regarding the best element and multiple True statuses can occur.

        Here we also keep the nearest element value and is done by reverse iterating in the zip function
        """

        # First, build all the basis weights and determine if it was an interior or not
        t0 = tm.time()
        kmax = agresults['number_neighbors']
        j = agresults['elements']
        phival_list = list()
        within_interior = list()

        for k_value in range(0, kmax):
            phival = self.basis2d(agdict, xylist, j[:, k_value])
            phival_list.append(phival)
            within_interior.append(self.basis2d_withinElement(phival))

        # detailed_weights_elements(phival_list, j)

        # Second only retain the "interior" results or nans if none
        final_weights = np.full((phival_list[0].shape[0], phival_list[0].shape[1]), np.nan)
        final_jvals = np.full(j.T[0].shape[0], -99999)
        final_status = np.full(within_interior[0].shape[0], False)

        # Loop backwards. thus keeping the "nearest" True for each geopoints for each k in kmax
        for pvals, jvals, testvals in zip(phival_list[::-1], j.T[::-1], within_interior[::-1]):  # THis loops over Kmax values
            final_weights[testvals] = pvals[testvals]
            final_jvals[testvals] = jvals[testvals]
            final_status[testvals] = testvals[testvals]

        agresults['final_weights'] = final_weights
        agresults['final_jvals'] = final_jvals
        agresults['final_status'] = final_status
        self.logger.info('Compute of basis took: %s seconds', tm.time() - t0)

        # Keep the list if the user needs to know after the fact
        outside_elements = np.argwhere(np.isnan(final_weights).all(axis=1)).ravel()
        agresults['outside_elements'] = outside_elements
        return agresults

    def detailed_weights_elements(self, phival_list, j):
        """
        This is only used for understanding better the detailed behavior of a particular grid
        It is not invoked for general use
        """
        for pvals, jvals in zip(phival_list, j.T):
            df_pvals = pd.DataFrame(pvals, columns=['Phi0', 'Phi1', 'Phi2'])
            df_pvals.index = df_pvals.index
            df_jvals = pd.DataFrame(jvals + 1, columns=['Element+1'])
            df = pd.concat([df_pvals, df_jvals], axis=1)
            df.index = df.index + 1
            df.index = df.index.astype(int)

            self.logger.debug('Dataframe: %s', df.loc[2].to_frame().T)

    @staticmethod
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
            for index, dataseries, weights in zip(range(0, len(data_list)), data_list, final_weights):
                reduced_data = np.matmul(dataseries.values, weights.T)
                df = pd.DataFrame(reduced_data, index=t, columns=[f'P{index + 1}'])
                final_list.append(df)
            df_final_data = pd.concat(final_list, axis=1)
        except Exception as e:
            df_final_data = None

        return df_final_data

    def WaterLevelSelection(self, t, data_list, final_weights):
        """
        Each data_list is a df for a single point containing three columns, one for each node in the containing element.
        We choose the first column in the list that has any number of values.
        Moving forward, one can make this approach better by choosing the highest weighted object with actual values

        A final df is returned with index=time and a single column for each of the
        input test points (some of which may be partially or completely nan)
        """
        final_list = list()

        # Index is a loop over multiple possible lon/lat pairs
        for index, dataseries, weights in zip(range(0, len(data_list)), data_list, final_weights):
            df_single = pd.DataFrame(index=t)
            count = 0

            for vertex in dataseries.columns:  # Loop over the 3 vertices and their weights in order
                count += 1
                df_single[f'P{vertex}'] = dataseries[vertex].values
                if df_single.count()[0] > 0:  # df.notna().sum()
                    final_list.append(df_single)
                    self.logger.debug('Inserted one chosen df_single with non nan values for index  %s at count number %s', index, count)
                    break

        self.logger.debug('Do Selection water series update')
        try:
            df_final_data = pd.concat(final_list, axis=1)
        except Exception as e:
            df_final_data = None
            self.logger.debug('This Exception usually simply means no data at the chosen lon/lat. But Exception is %s', e)

        return df_final_data

    @staticmethod
    def GenerateMetadata(agresults):
        """
        Here we want to simply help the user by reporting back the lon/lat values for each geo-point.
        This should be the same as the input dataset.

        -99999 indicates an element was not found in the grid.
        """

        df_lonlat = pd.DataFrame(agresults['geopoints'], columns=['LON', 'LAT'])
        df_elements = pd.DataFrame(agresults['final_jvals'] + 1, columns=['Element (1-based)'])
        df_elements.replace(-99998, -99999, inplace=True)
        df_meta = pd.concat([df_lonlat, df_elements], axis=1)
        df_meta['Point'] = df_meta.index + 1
        df_meta.set_index('Point', inplace=True)
        df_meta.rename('P{}'.format, inplace=True)

        return df_meta

    def ConstructReducedWaterLevelData_from_ds(self, ds, agdict, agresults, variable_name=None):
        """
        This method acquires ADCIRC water levels for the list of geopoints/elements.
        For each specified point in the grid, the resulting time series are reduced to a single time series using
        a (basis 2d) weighted sum.

        For a non-nan value to result in the final data, the product data must:
        1) Be non-nan for each time series at a specified time tick
        2) The test point must be interior to the specified element
        """

        if variable_name is None:
            raise 'User MUST supply the correct variable name'

        self.logger.debug('Variable name is: %s', variable_name)
        t0 = tm.time()

        data_list = list()
        t1 = tm.time()
        final_weights = agresults['final_weights']
        final_jvals = agresults['final_jvals']
        self.logger.debug('Time to acquire weigths and jvals: %s', tm.time() - t1)

        t1 = tm.time()
        acdict = self.get_adcirc_time_from_ds(ds)
        t = acdict['time'].values
        e = agdict['ele'].values
        self.logger.debug('Time to acquire time and element values: %s', tm.time() - t1)

        self.logger.debug('Before removal of out-of-triangle jvals: %s', final_jvals.shape)

        mask = (final_jvals == -99999)
        final_jvals = final_jvals[~mask]
        self.logger.debug(f'After removal of out-of-triangle jvals: %s', final_jvals.shape)

        t1 = tm.time()
        for vstation in final_jvals:
            advardict = self.get_adcirc_slice_from_ds(ds, variable_name, it=e[vstation])
            df = pd.DataFrame(advardict['var'])
            data_list.append(df)

        self.logger.debug('Time to TDS fetch annual all test station (triplets) was: %s seconds', tm.time() - t1)

        # logger.info('Selecting the weighted mean time series')
        # df_final=WaterLevelReductions(t, data_list, final_weights)

        self.logger.debug('Selecting the greedy alg: first in list with not all nans time series')
        df_final = self.WaterLevelSelection(t, data_list, final_weights)

        t0 = tm.time()
        df_meta = self.GenerateMetadata(agresults)  # This is here mostly for future considerations
        self.logger.debug('Time to reduce annual: %s, test stations is: %s seconds', len(final_jvals), tm.time() - t0)
        agresults['final_reduced_data'] = df_final
        agresults['final_meta_data'] = df_meta

        return agresults

    # NOTE We do not need to rebuild the tree for each year since the grid is unchanged.
    def Combined_pipeline(self, url, variable_name, lon, lat, nearest_neighbors=10):
        """
        Interpolate for one year.

        df_excluded_geopoints lists only those stations excluded by element tests.
        Some could be all nans due to dry points

        No flanks removed in this method as the caller may want to see everything
        """
        t0 = tm.time()
        geopoints = np.array([[lon, lat]])
        ds = self.f63_to_xr(url)
        agdict = self.get_adcirc_grid_from_ds(ds)
        agdict = self.attach_element_areas(agdict)

        self.logger.info('Compute_pipeline initiation: %s seconds', tm.time() - t0)
        self.logger.info('Start annual KDTree pipeline LON: %s LAT: %s', geopoints[0][0], geopoints[0][1])

        agdict = self.ComputeTree(agdict)
        agresults = self.ComputeQuery(geopoints, agdict, kmax=nearest_neighbors)
        agresults = self.ComputeBasisRepresentation(geopoints, agdict, agresults)
        agresults = self.ConstructReducedWaterLevelData_from_ds(ds, agdict, agresults, variable_name=variable_name)

        self.logger.debug('Basis function Tolerance value is: %s', self.TOL)
        self.logger.debug('List of %s stations not assigned to any grid element follows for kmax: %s', len(agresults["outside_elements"]),
                          nearest_neighbors)

        t0 = tm.time()
        df_product_data = agresults['final_reduced_data']
        df_product_metadata = agresults['final_meta_data']
        df_excluded_geopoints = pd.DataFrame(geopoints[agresults['outside_elements']], index=agresults['outside_elements'] + 1,
                                             columns=['lon', 'lat'])

        self.logger.debug('Compute_pipeline cleanup: %s seconds', tm.time() - t0)
        self.logger.debug('Finished annual Combined_pipeline')

        return df_product_data, df_product_metadata, df_excluded_geopoints

    @staticmethod
    def is_hurricane(test_val) -> bool:
        """
        Determine of the input test val is a Date, an Int or something else
        Parameters:
            test_val: For a valid time enter a str with dformat %Y-%m-%d %H:%M:%S or %Y%m%d%H
                      For a valid hurricane enter an int
        """
        is_hurricane = False

        try:
            test = dt.datetime.strptime(test_val, '%Y-%m-%d %H:%M:%S')  # If fails then not a datetime
        except (ValueError, TypeError):
            try:
                test = dt.datetime.strptime(test_val, '%Y%m%d%H')
            except Exception:
                try:
                    out_id = int(test_val)
                    is_hurricane = True
                except ValueError:
                    raise f'test indicates not a hurricane nor a casting. Perhaps a format issue?. Got {test_val}: Abort'

        return is_hurricane

    def generate_six_hour_time_steps_from_range(self, time_range) -> list:
        """
        Given the input time tuple, return the inclusive set of times that occur on
        the daily 6-hour mark. So on output we would have 00Z,06Z,12Z,18Z times only

        Parameters:
            time_range: Tuple (date time,date time) of the start and end times (datetime objects)

        Returns:
            list_of_times: list of (str) times in the format: %Y%m%d%H

        """
        if self.is_hurricane(time_range[0]):
            self.logger.debug('Determined input time_range URL is a Hurricane')
            list_of_times = self.generate_six_hour_time_advisories_from_range(time_range)
        else:
            list_of_times = self.generate_six_hour_time_castings_from_range(time_range)

        return list_of_times

    def generate_six_hour_time_castings_from_range(self, time_range) -> list:
        """
        A non-hurricane
        Advisory. We need to distinguish between the two. Note, we can be promiscuous here
        with the URLs, since urls that do not exist will get trapped by Harvester
        Parameters:
            time_range: tuple (datetime, datetime)
        Returns:
            list_of_times: list of times/advisories in a string format to build new urls
       """

        start_time = dt.datetime.strptime(time_range[0], '%Y-%m-%d %H:%M:%S')
        stop_time = dt.datetime.strptime(time_range[1], '%Y-%m-%d %H:%M:%S')
        pd_time = pd.date_range(start=start_time, end=stop_time, freq='h')  # Doesnt land on 00,06,12,18

        list_of_times = list()

        for time in pd_time:
            if time.hour in self.keep_hours:
                list_of_times.append(time.strftime('%Y%m%d%H'))

        # Keep input entry as well?
        list_of_times.append(stop_time.strftime('%Y%m%d%H'))
        list_of_times.sort()

        return list_of_times

    @staticmethod
    def generate_six_hour_time_advisories_from_range(advisory_range) -> list:
        """
        Advisory range has no specific time meaning other than generally being every 6 hours
        So simply accept the range as fact. The INPUT advisory number is NOT retained in the
        generated list

        Save Advisories in a leading zero format: "{:02d}".format(adv)

        Parameters:
            advisory_range: tuple (int,int)
        Returns:
            list_of_advisories: list of times/advisories in a string format to build new urls
                                includes the input time_step.advisory in the final list
       """
        # How many 6-hour periods can we identify? We need to choose a startpoint. Use the highest time and look back
        start_adv = int(advisory_range[0])
        stop_adv = int(advisory_range[1])

        if start_adv > stop_adv:
            start_adv, stop_adv = stop_adv, start_adv

        list_of_advisories = list()
        for inc in range(start_adv, stop_adv):
            list_of_advisories.append("{:02d}".format(inc))

        list_of_advisories = [i for i in list_of_advisories if int(i) > 0]

        # Should we retain the input value?
        list_of_advisories.append("{:02d}".format(stop_adv))

        # A last ditch sort to be sure
        list_of_advisories.sort()

        return list_of_advisories

    # Generates a proper list-time/advisories depending if its a Hurricane or not
    def generate_six_hour_time_steps_from_offset(self, time_value, offset) -> list:
        """
        For an arbitrary URL, we could have a conventional now/forecast OR a Hurricane
        Advisory. We need to distinguish between the two. Note, we can be promiscuous here
        with the URLs, since urls that do not exist will get trapped by Harvester
        Parameters:
            time_value: (datetime) Either the time or advisory value from a asgs url
            offset: (int) Number of DAYS to look back/forward from offset
                if offset is an Advisory then we look back a number of STEPS
                corresponding to 6 hour intervals based on offset
        Returns:
            list_of_times: list of times/advisories in a string format to build new urls
        """
        if self.is_hurricane(time_value):
            self.logger.debug('Determined input URL is a Hurricane')
            list_of_times = self.generate_six_hour_time_advisories_from_offset(time_value, offset)
        else:
            list_of_times = self.generate_six_hour_time_castings_from_offset(time_value, offset)

        return list_of_times

    def generate_six_hour_time_castings_from_offset(self, time_value, offset) -> list:
        """
        Start with the str_time and build a list of 6-hour steps for up to offset days
        We expect the input time to a stop_time and the offsets to be < 0. But, though
        the overall code has not been tested for it, we simply reorder the times
        as necessary and proceed

        Parameters:
           time_value: (datetime) start time
           offset: (int) Number of DAYS to look back/forward from time_value

        Returns:
            time_list: list of times in a string format to build new urls
        """
        stop_time = dt.datetime.strptime(time_value, '%Y-%m-%d %H:%M:%S')
        start_time = stop_time + dt.timedelta(days=offset)

        if start_time > stop_time:
            self.logger.warning('Stoptime < starttime. Supplied offset was %s days: Reordering', offset)
            start_time, stop_time = stop_time, start_time

        return self.generate_six_hour_time_steps_from_range((start_time.strftime('%Y-%m-%d %H:%M:%S'), stop_time.strftime('%Y-%m-%d %H:%M:%S')))

    @staticmethod
    def generate_six_hour_time_advisories_from_offset(str_time, offset) -> list:
        """
        Start with the str_time and build a list of 6-hour steps for up to offset days
        We expect the input time to bve an Advisory number (int). We also anticipate offsets to be < 0.
        since offset >0 would apply to Hurricane advisories not performed (but you could do it)

        Here we assume each index is a 6-hour time step. So we just need to decide how many to look back for.
        Harvester will quietly ignore urls that do not exist

        Save Advisories in a leading zero-padded format: "{:02d}".format(adv)

        Parameters:
           str_time: (str) time
           offset: (int) Number of DAYS to look back/forward from str_time

        Returns:
            list_of_advisories: list of advisories in a string format to build new urls
        """
        list_of_advisories = list()
        stop_advisory = int(str_time)
        num_6hour_look_asides = int(24 * offset / 6)
        range_values = [0, num_6hour_look_asides]
        range_values.sort()  # sorts ascending order

        for inc in range(*range_values):
            list_of_advisories.append("{:02d}".format(stop_advisory + inc))

        list_of_advisories = [i for i in list_of_advisories if int(i) >= 0]

        # Keep the input value?
        list_of_advisories.append("{:02d}".format(stop_advisory))

        # A last ditch sort to be sure
        list_of_advisories.sort()

        return list_of_advisories

    @staticmethod
    def grab_years_from_time_list(list_of_times) -> list:
        """
        Process the input time list to extract a list of Years (str)
        Note: This could be a list of Advisories as well. If so,

        return the advisory number, though it will probably not be used

        Parameters:
            list_of_times: List of (str) time in the format %Y%m%d%H
        Returns:
            list of year values

        """
        list_of_years = list()

        for time in list_of_times:
            try:
                value = dt.datetime.strptime(time, '%Y%m%d%H').year
            except TypeError:
                value = time
            list_of_years.append(value)

        return list_of_years

    @staticmethod
    def generate_list_of_instances(list_of_times, in_gridname, in_instance):
        """
        This function matches every entry in the list_of_times with an associated instance.
        The structure of this code is such that, in the future, we may have scenarios where
        the value of the instance may change for a given year.

        Currently, though, we will simply build a list of identical instances.
        The value of the selected instance may be passed in by the caller

        Parameters:
           list_of_times: list (str)(%Y%m%d%H) ordered set of instances from which to build new URLs
           in_gridname: current gridname from a representative INPUT url
           in_gridname: current instance from a representative INPUT url

        Returns:
            instance_list: ordered list of instances to use for building a set of new urls.
        """
        num_entries = len(list_of_times)

        gridname = in_gridname  # Get default values
        instance = in_instance

        instance_list = num_entries * [instance]

        return instance_list


    # Expect this to be part of a looped  list of times from which appending will be applied
    def construct_url_from_yaml(self, config, intime, instance, ensemble, gridname, hurricane_yaml_year=None, hurricane_yaml_source=None):
        """
        Given a single time (%Y%m%d%H) or advisory, the gridname, instance, and ensemble values
        use the entries in config to build a proper URL
        If applying to Hurricanes, we need to also applyld_url_list_from_yaml_and_timest the values for hurricane_yaml_year, and
        hurricane_yaml_source
        """
        # hurricane_yaml_source is a special case scenario
        if self.is_hurricane(intime):
            self.logger.debug('Request for YAML build of Hurricane URL. subdir is %s', hurricane_yaml_source)
            intime = str(intime)
            subdir = hurricane_yaml_year  # This is certainly NOT generalized
            source = hurricane_yaml_source
        else:
            subdir = dt.datetime.strptime(intime, '%Y%m%d%H').year
            source = 'nam'

        cfg = config['ADCIRC']
        url = cfg["baseurl"] + cfg["dodsCpart"] % (
        subdir, source, intime, cfg["AdcircGrid"] % (gridname), cfg["Machine"], cfg["Instance"] % (instance), cfg["Ensemble"] % (ensemble),
        cfg["fortNumber"])

        return url

    def construct_start_time_from_offset(self, stop_time, n_days):
        """
        Construct an appropriate start_time given the stop_time and offset.
        NOTE if this is a Hurricane advisory, we return an appropriate
        advisory assuming each advisory is 6 hours in duration. No
        negative advisories are returned

        Parameters:
            stop_time (str) (%Y-%m-%d %H:%M:%S)
            n_days: (int) number of 24-hour days to look back/forward

        """
        try:

            if self.is_hurricane(stop_time):
                num_6hour_look_asides = int(24 * n_days / 6)
                stop_adv = int(stop_time)
                start_adv = stop_adv + num_6hour_look_asides  # We normally assume offset is negative but that is not enforced
                return start_adv
            else:
                t_stop = dt.datetime.strptime(stop_time, '%Y-%m-%d %H:%M:%S')
                t_start = t_stop + dt.timedelta(days=n_days)
                start_time = t_start.strftime('%Y-%m-%d %H:%M:%S')
                return start_time
        except Exception:
            raise 'Fell out the bottom of construct_start_time_from_offset. Abort'
