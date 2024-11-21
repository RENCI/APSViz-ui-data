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
import re
import time as tm
import datetime as dt
import numpy as np
import pandas as pd
import xarray as xr

from scipy import spatial as sp


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
        # get a handle to a logger
        self.logger = _logger

        self.k_max = 10
        self.got_kdtree = None
        self.tol = 10e-5
        self.debug = True  # False

        # Specify available reanalysis years
        self.y_min = 1979
        self.y_max = 2023
        self.years = list(range(self.y_min, self.y_max + 1))

        # logger.debug('utilities:y_min, y_max: %s, %s', y_min,y_max)

        self.file_ext = '.d0.no-unlim.T.rc.nc'
        # self.file_ext='.d4.no-unlim.T.rc.nc';
        # self.logger.debug('utilities:file_ext: %s', file_ext)

        # Default standard location is on the primary RENCI TDS
        # self.url_dir_format="https://tds.renci.org/thredds/dodsC/Reanalysis/ADCIRC/ERA5/hsofs/%d-post"
        # self.url_dir_format="https://tds.renci.org/thredds/dodsC/Reanalysis/ADCIRC/ERA5/ec95d/%d"
        self.url_dir_format = "https://tdsres.apps.renci.org/thredds/dodsC/ReanalysisV2/ADCIRC/ERA5/hsofs.V2/%d-post"

        self.keep_hours = [0, 6, 12, 18]

    @staticmethod
    def get_adcirc_grid_from_ds(ds):
        """
            creates an ad dict
        """
        ag_dict: dict = {'lon': ds['x'][:], 'lat': ds['y'][:], 'ele': ds['element'][:, :] - 1, 'depth': ds['depth'][:], 'latmin': np.mean(ds['y'][:])}

        return ag_dict

    @staticmethod
    def attach_element_areas(ag_dict):
        """
        gets the element areas
        """
        x = ag_dict['lon'].values
        y = ag_dict['lat'].values
        e = ag_dict['ele'].values

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

        ag_dict['areas'] = (x1 * dy23 + x2 * dy31 + x3 * dy12) / 2.
        ag_dict['edge_lengths'] = [a, b, c]
        ag_dict['dl'] = np.mean(ag_dict['edge_lengths'], axis=0)

        return ag_dict

    def basis2d_within_element(self, phi):
        """
        gets the basis 2d elements
        """
        interior_status = np.all(phi[:] <= 1 + self.tol, axis=1) & np.all(phi[:] >= 0 - self.tol, axis=1)

        return interior_status

    @staticmethod
    def basis2d(ag_dict, xy_list, j):
        """
        performs basis 2D operations

        """
        # check length of j and xy_list
        # check for the necessary arrays in ag_dict

        # nodes for the elements in j
        n3 = ag_dict['ele'][j]

        x = ag_dict['lon'][n3].values
        x1 = x[:, 0]
        x2 = x[:, 1]
        x3 = x[:, 2]

        y = ag_dict['lat'][n3].values
        y1 = y[:, 0]
        y2 = y[:, 1]
        y3 = y[:, 2]

        area_j = ag_dict['areas'][j]
        xp = xy_list[:, 0]
        yp = xy_list[:, 1]

        # Basis function 1
        a = (x2 * y3) - (x3 * y2)
        b = y2 - y3
        c = -(x2 - x3)
        phi0 = (a + b * xp + c * yp) / (2.0 * area_j)

        # Basis function 2
        a = (x3 * y1) - (x1 * y3)
        b = y3 - y1
        c = -(x3 - x1)
        phi1 = (a + b * xp + c * yp) / (2.0 * area_j)

        # Basis function 3
        a = (x1 * y2) - (x2 * y1)
        b = y1 - y2
        c = -(x1 - x2)
        phi2 = (a + b * xp + c * yp) / (2.0 * area_j)

        return np.array([phi0, phi1, phi2]).T

    @staticmethod
    def get_adcirc_time_from_ds(ds):
        """
        gets the ADCIRC time from the dataset
        """
        return {'time': ds['time']}

    @staticmethod
    def f63_to_xr(url):
        """
        returns the dataset without certain variables
        """
        dropvars = ['neta', 'nvel', 'max_nvdll', 'max_nvell']

        return xr.open_dataset(url, drop_variables=dropvars)

    @staticmethod
    def get_adcirc_slice_from_ds(ds, v, it=0):
        """
        gets ADCIRC data from the dataset
        """
        ad_var_dict = {}

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
                raise Exception(f'Unexpected leading variable name: {ds.variables[v].dims}. Abort')

        # var_d[var_d.mask] = np.nan
        ad_var_dict['var'] = var_d.data

        return ad_var_dict

    def compute_tree(self, ag_dict):
        """
        Given lon,lat,ele in ag_dict,compute element centroids and
        generate the ADCIRC grid KDTree
        returns ag_dict with tree
        """

        t0 = tm.time()

        try:
            x = ag_dict['lon'].values.ravel()  # ravel; not needed
            y = ag_dict['lat'].values.ravel()
            e = ag_dict['ele'].values
        except Exception as e:
            raise Exception('Did not find lon,lat,ele data in ag_dict.') from e

        xe = np.mean(x[e], axis=1)
        ye = np.mean(y[e], axis=1)

        # Still want to build up the data for ag_dict, we just do not need the tree reevaluated for every year
        if self.got_kdtree is None:
            ag_dict['tree'] = tree = sp.KDTree(np.c_[xe, ye])
            self.got_kdtree = tree
        else:
            ag_dict['tree'] = self.got_kdtree

        self.logger.debug('Build annual KDTree time is: %s seconds', tm.time() - t0)

        return ag_dict

    def compute_query(self, xy_list, ag_dict, kmax=10):
        """
        Generate the kmax-set of nearest neighbors to each lon,lat pair in xylist.
        Each test point (each lon/lat pair) gets associated distance (dd) and element (j) objects
        At this stage it is possible that some test points are not interior to the nearest element.
        We will subsequently check that.

        dd: num points by neighbors
        j: num points by neighbors
        """
        t0 = tm.time()
        ag_results = {}

        dd, j = ag_dict['tree'].query(xy_list, k=kmax)

        if kmax == 1:
            dd = dd.reshape(-1, 1)
            j = j.reshape(-1, 1)

        ag_results['distance'] = dd
        ag_results['elements'] = j
        ag_results['number_neighbors'] = kmax
        ag_results['geopoints'] = xy_list  # We shall use this later

        self.logger.debug('KDTree query of size: %s took: %s seconds', kmax, tm.time() - t0)

        return ag_results

    def compute_basis_representation(self, xy_list, ag_dict, ag_results):
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
        kmax = ag_results['number_neighbors']
        j = ag_results['elements']
        phival_list = []
        within_interior = []

        for k_value in range(0, kmax):
            phi_val = self.basis2d(ag_dict, xy_list, j[:, k_value])
            phival_list.append(phi_val)
            within_interior.append(self.basis2d_within_element(phi_val))

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

        ag_results['final_weights'] = final_weights
        ag_results['final_jvals'] = final_jvals
        ag_results['final_status'] = final_status
        self.logger.info('Compute of basis took: %s seconds', tm.time() - t0)

        # Keep the list if the user needs to know after the fact
        outside_elements = np.argwhere(np.isnan(final_weights).all(axis=1)).ravel()
        ag_results['outside_elements'] = outside_elements
        return ag_results

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
    def water_level_reductions(t, data_list, final_weights):
        """
        Each data_list is a df for a single point containing 3 columns, one for
        each node in the containing element.
        These columns are reduced using the final_weights previously calculated

        A final df is returned with index=time and a single column for each of the
        input test points (some of which may be partially or completely nan)
        """
        try:
            final_list = []

            for index, dataseries, weights in zip(range(0, len(data_list)), data_list, final_weights):
                reduced_data = np.matmul(dataseries.values, weights.T)
                df = pd.DataFrame(reduced_data, index=t, columns=[f'P{index + 1}'])
                final_list.append(df)

            df_final_data = pd.concat(final_list, axis=1)
        except Exception:
            df_final_data = None

        return df_final_data

    def water_level_selection(self, t, data_list, final_weights):
        """
        Each data_list is a df for a single point containing three columns, one for each node in the containing element.
        We choose the first column in the list that has any number of values.
        Moving forward, one can make this approach better by choosing the highest weighted object with actual values

        A final df is returned with index=time and a single column for each of the
        input test points (some of which may be partially or completely nan)
        """
        final_list = []

        # Index is a loop over multiple possible lon/lat pairs
        for index, data_series, weights in zip(range(0, len(data_list)), data_list, final_weights):
            df_single = pd.DataFrame(index=t)
            count = 0

            for vertex in data_series.columns:  # Loop over the 3 vertices and their weights in order
                count += 1
                df_single[f'P{vertex}'] = data_series[vertex].values
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
    def generate_metadata(ag_results):
        """
        Here we want to simply help the user by reporting back the lon/lat values for each geo-point.
        This should be the same as the input dataset.

        -99999 indicates an element was not found in the grid.
        """

        df_lonlat = pd.DataFrame(ag_results['geopoints'], columns=['LON', 'LAT'])
        df_elements = pd.DataFrame(ag_results['final_jvals'] + 1, columns=['Element (1-based)'])
        df_elements.replace(-99998, -99999, inplace=True)

        df_meta = pd.concat([df_lonlat, df_elements], axis=1)
        df_meta['Point'] = df_meta.index + 1
        df_meta.set_index('Point', inplace=True)
        df_meta.rename('P{}'.format, inplace=True)

        return df_meta

    def construct_reduced_water_level_data_from_ds(self, ds, ag_dict, ag_results, variable_name=None):
        """
        This method acquires ADCIRC water levels for the list of geopoints/elements.
        For each specified point in the grid, the resulting time series is reduced to a single time series using
        a (basis 2d) weighted sum.

        For a non-nan value to result in the final data, the product data must:
        1) Be non-nan for each time series at a specified time tick
        2) The test point must be inside the specified element
        """

        if variable_name is None:
            raise Exception('User MUST supply the correct variable name')

        self.logger.debug('Variable name is: %s', variable_name)
        t0 = tm.time()

        data_list = []
        t1 = tm.time()
        final_weights = ag_results['final_weights']
        final_jvals = ag_results['final_jvals']
        self.logger.debug('Time to acquire weigths and jvals: %s', tm.time() - t1)

        t1 = tm.time()
        ac_dict = self.get_adcirc_time_from_ds(ds)
        t = ac_dict['time'].values
        e = ag_dict['ele'].values
        self.logger.debug('Time to acquire time and element values: %s', tm.time() - t1)

        self.logger.debug('Before removal of out-of-triangle jvals: %s', final_jvals.shape)

        mask = final_jvals == -99999
        final_jvals = final_jvals[~mask]
        self.logger.debug('After removal of out-of-triangle jvals: %s', final_jvals.shape)

        t1 = tm.time()
        for v_station in final_jvals:
            ad_vardict = self.get_adcirc_slice_from_ds(ds, variable_name, it=e[v_station])
            df = pd.DataFrame(ad_vardict['var'])
            data_list.append(df)

        self.logger.debug('Time to TDS fetch annual all test station (triplets) was: %s seconds', tm.time() - t1)

        # logger.info('Selecting the weighted mean time series')
        # df_final=WaterLevelReductions(t, data_list, final_weights)

        self.logger.debug('Selecting the greedy alg: first in list with not all nans time series')
        df_final = self.water_level_selection(t, data_list, final_weights)

        t0 = tm.time()
        df_meta = self.generate_metadata(ag_results)  # This is here mostly for future considerations
        self.logger.debug('Time to reduce annual: %s, test stations is: %s seconds', len(final_jvals), tm.time() - t0)
        ag_results['final_reduced_data'] = df_final
        ag_results['final_meta_data'] = df_meta

        return ag_results

    # NOTE We do not need to rebuild the tree for each year since the grid is unchanged.
    def combined_pipeline(self, url, variable_name, lon, lat, nearest_neighbors=10):
        """
        Interpolate for one year.

        df_excluded_geopoints lists only those stations excluded by element tests.
        Some could be all nans due to dry points

        No flanks removed in this method as the caller may want to see everything
        """
        t0 = tm.time()
        geopoints = np.array([[lon, lat]])
        ds = self.f63_to_xr(url)
        ag_dict = self.get_adcirc_grid_from_ds(ds)
        ag_dict = self.attach_element_areas(ag_dict)

        self.logger.info('Compute_pipeline initiation: %s seconds', tm.time() - t0)
        self.logger.info('Start annual KDTree pipeline LON: %s LAT: %s', geopoints[0][0], geopoints[0][1])

        ag_dict = self.compute_tree(ag_dict)
        ag_results = self.compute_query(geopoints, ag_dict, kmax=nearest_neighbors)
        ag_results = self.compute_basis_representation(geopoints, ag_dict, ag_results)
        ag_results = self.construct_reduced_water_level_data_from_ds(ds, ag_dict, ag_results, variable_name=variable_name)

        self.logger.debug('Basis function Tolerance value is: %s', self.tol)
        self.logger.debug('List of %s stations not assigned to any grid element follows for kmax: %s', len(ag_results["outside_elements"]),
                          nearest_neighbors)

        t0 = tm.time()
        df_product_data = ag_results['final_reduced_data']
        df_product_metadata = ag_results['final_meta_data']
        df_excluded_geopoints = pd.DataFrame(geopoints[ag_results['outside_elements']], index=ag_results['outside_elements'] + 1,
                                             columns=['lon', 'lat'])

        self.logger.debug('Compute_pipeline cleanup: %s seconds', tm.time() - t0)
        self.logger.debug('Finished annual Combined_pipeline')

        return df_product_data, df_excluded_geopoints  # , df_product_metadata

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
                except ValueError as e:
                    raise ValueError(f'test indicates not a hurricane nor a casting. Perhaps a format issue?. Got {test_val}: Abort') from e

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

        list_of_times = []

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

        list_of_advisories = []
        for inc in range(start_adv, stop_adv):
            list_of_advisories.append(f'{inc:02d}')

        list_of_advisories = [i for i in list_of_advisories if int(i) > 0]

        # Should we retain the input value?
        list_of_advisories.append(f'{stop_adv:02d}')

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
        list_of_advisories = []
        stop_advisory = int(str_time)
        num_6hour_look_asides = int(24 * offset / 6)
        range_values = [0, num_6hour_look_asides]
        range_values.sort()  # sorts ascending order

        for inc in range(*range_values):
            list_of_advisories.append(f'{stop_advisory + inc:02d}')

        list_of_advisories = [i for i in list_of_advisories if int(i) >= 0]

        # Keep the input value?
        list_of_advisories.append(f'{stop_advisory:02d}')

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
        list_of_years = []

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
           :param list_of_times: list (str)(%Y%m%d%H) ordered set of instances from which to build new URLs
           :param in_gridname: current gridname from a representative INPUT url
           :param in_instance: current instance from a representative INPUT url

        Returns:
            instance_list: ordered list of instances to use for building a set of new urls.
        """
        num_entries = len(list_of_times)

        # gridname = in_gridname  # Get default values
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

            t_stop = dt.datetime.strptime(stop_time, '%Y-%m-%d %H:%M:%S')
            t_start = t_stop + dt.timedelta(days=n_days)
            start_time = t_start.strftime('%Y-%m-%d %H:%M:%S')

            return start_time

        except Exception as e:
            raise Exception('Fell out the bottom of construct_start_time_from_offset. Abort') from e
