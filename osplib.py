##############################################################################
############################################################################## 
# Open Sonar Library
##############################################################################
##############################################################################
# Created for the Open Sonar Project by:
# Graham Christie, Isaac Fuller, Kara Sanford
# January 2022
#############################################
# Version 5.0
##############################################################################
##############################################################################

import csv
import os
import io
import numpy
import datetime as dt
import serial

import geopy.distance

from brping import Ping1D
import pynmea2

#########################################
#########################################
# Sensors
#########################################
#########################################

#-----------------------------------------------------------------------------
class GNSS:
# Class to manage all GNSS functions
    def __init__(self, metadata):
        self.gps_com = metadata['GNSS_Com'][1]
        self.gps_baud = metadata['GNSS_Com'][2]
    
    def connect_gnss(self):
        # Function to connect to the GNSS serial port
        try:
            self.gps_ser = serial.Serial(self.gps_com, self.gps_baud, timeout=0.1)
            self.gps_sio = io.TextIOWrapper(io.BufferedRWPair(self.gps_ser, self.gps_ser))
            self.gps_found = True
            print('GNSS Connected')
            return self.gps_found
        except:
            print('No GNSS Found!')
            self.gps_found = False
            return self.gps_found
        
    def disconnect_gnss(self):
        # Function to close serial port to GNSS
        if not self.gps_found:
            return
        self.gps_ser.close()
        self.gps_found = False
            
    def get_nmea(self):
        # Function to get GGA strings with timestamp attached
        if not self.gps_found:
            gps_error = [['No','GNSS Found'],True]
            return gps_error
        while True:
            ping = False
            try:
                self.gps_ser.reset_input_buffer()
                line = self.gps_sio.readline()
                time = dt.datetime.utcnow().time()
                msg = pynmea2.parse(line)
                
            
                if type(msg) == pynmea2.types.talker.GGA:
                    ping = True
                elif type(msg) == pynmea2.types.talker.RMC:
                    ping = True
                elif type(msg) == pynmea2.types.talker.GLL:
                    ping = True
                return time, msg, ping
            except:
                pass
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
class Sonar:
# Class to manage all sonar functions
    def __init__(self, metadata):
        self.sonar_com = metadata['Sonar_Com'][1]
        self.sonar_baud = metadata['Sonar_Com'][2]
        
    def connect_sonar(self):
        # Function to connect to the sonar
        try:
            self.myping = Ping1D()
            self.myping.connect_serial(self.sonar_com, self.sonar_baud)
            self.sonar_found = True
            print('Sonar Connected')
            return self.sonar_found
        except:
            print('No Sonar Found!')
            self.sonar_found = False
            return self.sonar_found
        
    def send_ping(self):
        # Function to take a single ping from sonar
        if not self.sonar_found:
            return
        self.distance = self.myping.get_distance()
        self.distance['distance'] = self.distance['distance']/1000
        self.time = dt.datetime.utcnow().time()
        
        observation = self.time, self.distance
        
        return observation
            
    def ping_to_string(self, ssp):
        #Function to turn the observation dictionary into a string for writing
        if not self.sonar_found:
            return 'No Sonar Found'
        dist = self.distance['distance']
        conf = self.distance['confidence']
        dura = self.distance['transmit_duration']
        star = self.distance['scan_start']
        leng = self.distance['scan_length']
        gain = self.distance['gain_setting']
        
        ping_string = str(self.time)+','+'$DEPTH,'+str(dist)+','+\
            str(conf)+','+str(dura)+','+str(star)+','+str(leng)+','\
            +str(gain)+','+str(ssp) + '\n'
        
        return ping_string
    
    def set_sound_speed(self, soundspeed):
        # Function to set a specified sound speed to the sonar
        sound_speed_ms = soundspeed
        if not self.sonar_found:
            return False
        sound_speed_mms = round(sound_speed_ms * 1000)
        print('--------------------------------------------------------------')
        print(f'Setting sound speed to {sound_speed_ms} m/s on sonar head')
        print('--------------------------------------------------------------')
        self.myping.set_speed_of_sound(sound_speed_mms)
        return True
    
    def get_sound_speed(self):
        # Function to get the sound speed set onthe sonar
        if not self.sonar_found:
            return
        sound_speed_mms = self.myping.get_speed_of_sound()
        sound_speed_ms = sound_speed_mms['speed_of_sound']/1000
        print('--------------------------------------------------------------')
        print(f'Sound speed is to {sound_speed_ms} m/s on sonar head')
        print('--------------------------------------------------------------')
        return sound_speed_ms
        
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
class Speed:
# Class to manage all surface sound speed functions
    def __init__(self, metadata):
        self.svp_com = metadata['SVP_Com'][1]
        self.svp_baud = metadata['SVP_Com'][2]
        
    def connect_speed(self):
        # Function to connect to the surface svp
        try:
            self.svp_ser = serial.Serial(self.svp_com, self.svp_baud, timeout=2.5)
            self.svp_sio = io.TextIOWrapper(io.BufferedRWPair(self.svp_ser, self.svp_ser))
            self.svp_found = True
            print('SVP Connected')
            return self.svp_found
        except:
            print('No SVP Found!')
            self.svp_found = False
            return self.svp_found
        
    def get_surface_sound_speed(self):
        try:
            self.svp_ser.reset_input_buffer()
            line = self.svp_ser.readline()
            message = line.decode()
            soundspeed = float(message)/1000
            return soundspeed
        except:
            print('SVP Error')
            pass

    def disconnect_speed(self):
        # Function to close serial port to surface svp
        if not self.svp_found:
            return
        self.svp_ser.close()
#-----------------------------------------------------------------------------

#########################################
#########################################
# Online Actions
#########################################
#########################################

#-----------------------------------------------------------------------------
def take_observation(metadata, gnss_device, sonar_device, svp_device, 
                     current_speed, update_speed, obs_numb, simple_log, raw_log):
    if update_speed:
        if obs_numb == 100:
            print('Updating sound speed from sound velocity probe')
            current_speed = svp_device.get_surface_sound_speed()
            sonar_device.set_sound_speed(current_speed)
            
            obs_numb = 0
        else:
            obs_numb += 1
     
    time, nmea, ping = gnss_device.get_nmea()
        
    nmea_message = str(time) + ',' + str(nmea) + '\n'
    raw_log.write(nmea_message)
    if ping:
            
        time, sonar = sonar_device.send_ping()
            
        sonar_message = sonar_device.ping_to_string(current_speed)
            
        raw_log.write(sonar_message)
        nmea_message = nmea_message.split(',')
        if nmea_message[1] == '$GNGGA':
            waterline = metadata['Sonar'][2]
            depth = sonar['distance']+waterline
            depth = round(depth,3)
            height = float(nmea_message[10])+float(nmea_message[12])-depth-metadata['GNSS'][2]
            height = round(height,3)
            simple_message = str(time)+','+str(nmea.latitude)+','+\
                str(nmea.longitude)+','+str(depth)+','+\
                str(height)+','+str(current_speed)+'\n'
            print(simple_message)
            simple_log.write(simple_message)
    return obs_numb, current_speed
#-----------------------------------------------------------------------------

#########################################
#########################################
# Readers
#########################################
#########################################

#-----------------------------------------------------------------------------
def generic_reader(filename, delimit, start, end):
    # Generic reader used to read all files
    return_list = []        
    with open(filename) as file:
        csv_reader = csv.reader(file, delimiter=delimit)
        start_found = False
        for row in csv_reader:
            if row[0] != start and start_found == False:
                pass
            elif row[0] == start:
                start_found = True                    
            elif row[0] == end:                    
                return return_list
            else:
                return_list.append(row) 
    return return_list    
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def read_config_file(filename):
    # Controller for generic reader to extract metadata from config file
    if not file_check(filename, '.csv'):
        return
        
    read_result = generic_reader(filename,',','Header_Start','Header_End')
        
    meta = {}
    meta['Survey'] = [read_result[0][0],read_result[0][1],read_result[0][2]]
    meta['Geodetics'] = [float(read_result[1][0]),float(read_result[1][1])]
    meta['Vessel'] = [read_result[2][0]]
    meta['GNSS'] = [read_result[3][0],read_result[3][1],float(read_result[3][2]),
                        float(read_result[3][3]),float(read_result[3][4]),
                        float(read_result[3][5])]        
    meta['Sonar'] = [read_result[4][0],read_result[4][1],float(read_result[4][2]),
                         float(read_result[4][3]),float(read_result[4][4]),
                         float(read_result[4][5])] 
    meta['SVP'] = [read_result[5][0],read_result[5][1],float(read_result[5][2])]
    meta['GNSS_Com'] = [read_result[6][0],read_result[6][1], int(read_result[6][2])]
    meta['Sonar_Com'] = [read_result[7][0],read_result[7][1], int(read_result[7][2])]
    meta['SVP_Com'] = [read_result[8][0],read_result[8][1],int(read_result[8][2])]
                
    return meta
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def read_raw_log(filename):
    # Function to read raw logs using generic reader
    if not file_check(filename, '.csv'):
        return
    
    metadata = read_config_file(filename)
    data_return = generic_reader(filename, ',', 'Header_End', None)
    data = []
    
    for row in data_return:
        
        for i in range(len(row)):
            try:
                row[i] = float(row[i])
            except:
                pass
        
        data_line = {}
        if len(row) < 2:
            pass
        elif row[1] == '$DEPTH':
            if len(row) == 9:
                data_line['time'] = dt.datetime.strptime(row[0], '%H:%M:%S.%f')
                data_line['type'] = row[1]
                data_line['depth'] = row[2]/1000
                data_line['confidence'] = row[3]
                data_line['duration'] = row[4]
                data_line['start'] = row[5]/1000
                data_line['length'] = row[6]/1000
                data_line['gain'] = row[7]
                data_line['speed'] = row[8]
                
                data.append(data_line)
       
            
        elif row[1].endswith('VTG'):
            if len(row) == 11:
                data_line['time'] = dt.datetime.strptime(row[0], '%H:%M:%S.%f')
                data_line['type'] = 'VTG'    
                data_line['tmg_true'] = row[2]    
                data_line['true'] = row[3]    
                data_line['tmg_mag'] = row[4]    
                data_line['mag'] = row[5]    
                data_line['speed_kt'] = row[6]    
                data_line['knots'] = row[7]    
                data_line['speed_km'] = row[8]    
                data_line['km'] = row[9]    
                data_line['checksum'] = row[10]    
                
                data.append(data_line)

            
            
        elif row[1].endswith('RMC'):
            if len(row) == 15:
                data_line['time'] = dt.datetime.strptime(row[0], '%H:%M:%S.%f')
                data_line['type'] = 'RMC'    
                data_line['gps_time'] = row[2]    
                data_line['status'] = row[3]    
                data_line['lat'] = row[4]    
                data_line['lat_hem'] = row[5]    
                data_line['long'] = row[6]    
                data_line['long_hem'] = row[7]    
                data_line['speed_kt'] = row[8]    
                data_line['tmg_true'] = row[9]    
                data_line['date'] = row[10]    
                data_line['mag_var'] = row[11]
                data_line['checksum'] = row[12] 
                data.append(data_line)

        
        elif row[1].endswith('GGA'):
            if len(row) == 16:
                data_line['time'] = dt.datetime.strptime(row[0], '%H:%M:%S.%f')
                data_line['type'] = 'GGA'    
                data_line['utc'] = row[2]    
                data_line['lat'] = row[3]    
                data_line['lat_hem'] = row[4]    
                data_line['long'] = row[5]    
                data_line['long_hem'] = row[6]    
                data_line['quality'] = row[7]    
                data_line['sv_visible'] = row[8]    
                data_line['hdop'] = row[9]    
                data_line['ortho_height'] = row[10]    
                data_line['meters'] = row[11]
                data_line['geoid_sep'] = row[12]
                data_line['meters'] = row[13]
                data_line['gps_age'] = row[14]
                #data_line['ref_id'] = row[15]
                data_line['checksum'] = row[15]
                
                data.append(data_line)
            else:
                pass 
        
        elif row[1].endswith('GLL'):
            if len(row) == 9:
                data_line['time'] = dt.datetime.strptime(row[0], '%H:%M:%S.%f')
                data_line['type'] = 'GLL'    
                data_line['lat'] = row[2]    
                data_line['lat_hem'] = row[3]    
                data_line['long'] = row[4]    
                data_line['lon_hem'] = row[5]    
                data_line['utc'] = row[6]    
                data_line['status'] = row[7]    
                data_line['checksum'] = row[8]    
                data.append(data_line)
            else:
                pass 
            
        '''
        elif row[1].endswith('GSV'):
            if len(row) == 13:
                data_line['time'] = dt.datetime.strptime(row[0], '%H:%M:%S.%f')
                data_line['type'] = row[1]    
                data_line[''] = row[2]    
                data_line[''] = row[3]    
                data_line[''] = row[4]    
                data_line[''] = row[5]    
                data_line[''] = row[6]    
                data_line[''] = row[7]    
                data_line[''] = row[8]    
                data_line[''] = row[9]    
                data_line[''] = row[10]    
                data_line[''] = row[11]
                data_line[''] = row[12] 
                data.append(data_line)
            else:
                pass 
        '''        

    raw_log = {}
    raw_log['Metadata'] = metadata
    raw_log['Data'] = data
        
    return raw_log


#-----------------------------------------------------------------------------

#########################################
#########################################
# Writers
#########################################
#########################################

#-----------------------------------------------------------------------------
def write_meta_header(filename, metadata):
    # Function to write new files with survey metadata as the header
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([metadata['filetype']])
        writer.writerow(['Header_Start'])
        writer.writerow(metadata['Survey'])
        writer.writerow(metadata['Geodetics'])
        writer.writerow(metadata['Vessel'])
        writer.writerow(metadata['GNSS'])
        writer.writerow(metadata['Sonar'])
        writer.writerow(metadata['SVP'])
        writer.writerow(metadata['GNSS_Com'])
        writer.writerow(metadata['Sonar_Com'])
        writer.writerow(metadata['SVP_Com'])
        if metadata['filetype'][0] == 'OSP_SIMPLE_LOG':
            writer.writerow(['Time, Latitude, Longitude, Depth_Below_Water, Height_Ellipsoidal, Soundspeed'])
        writer.writerow(['Header_End'])
    print(metadata['filetype'][0]+' file created')    
        
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def write_hsx_body(raw_log):
    # Function to write the body of a file in the HSX format
    # UNFINISHED!!!!!!!!!!!!!!!!!!!!!!!!!
    for obs in raw_log[1]:
        if obs['type'] == 'GGA':
            lat = 0
            long = 0
            line = 'RAW '+dt.datetime.strftime(obs['time'],
                          '%H:%M:%S.%f')+' '+str(lat)+' '+str(long)+' '+\
                          str(obs['ortho_height'])+'\n'
            print(line)
            
#-----------------------------------------------------------------------------

#########################################
#########################################
# Miscelaneous applications
#########################################
#########################################

#-----------------------------------------------------------------------------
def file_check(filename, ending):
    # Checks if the filename exist and if they end in .csv
    if filename.endswith(ending):
        return True
    else:
        print('File does not end in ".csv"')
        return False
    filexists = os.path.isfile(filename)
    if not filexists:
        print('File not found!')
        return False
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------        
def compute_horizontal_offsets(logfile_offset, logfile_data):
    # Function to compute offset from GNSS antenna to sonar for every sounding          ################## Update to take raw file or readable file
    
    offset_xy = ((logfile_offset[0])**2)+((logfile_offset[1])**2)
    sonar_offset_distance = numpy.sqrt(offset_xy)
            
    num1 = logfile_offset[0]
    num2 = logfile_offset[1]

    if num2 == 0:
        num2 = 0.0000000000001

    bearing_raw = numpy.rad2deg(numpy.arctan(num1/num2))

    if num1 >= 0 and num2 >= 0:
        bearing = bearing_raw
    elif num1 < 0 and num2 > 0:
        bearing = 360 + bearing_raw
    else:
        bearing = 180 + bearing_raw
    
    bearing = numpy.round(bearing, 4)
    
    
    sonar_position_list = []
    for row in logfile_data:
        lat = row['Lat']
        long = row['Long']
        course = row['Course']
        
        course_adjusted = course + bearing
        if course_adjusted >=360:
            course_adjusted = course_adjusted - 360
        
        
        sonar_position = geopy.distance.distance(meters=sonar_offset_distance
                            ).destination((lat,long), bearing=course_adjusted)
        sonar_lat = sonar_position.latitude
        sonar_long = sonar_position.longitude
        sonar_position_list.append([sonar_lat,sonar_long])
          
    return sonar_position_list
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def remove_null_string(start_list, replacement):
    # Function that removes null values from a list and replaces with a value
    result_list = [str(x or replacement) for x in start_list]
    return result_list
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------        
def dms_to_dd(dms):
    # Takes an input of dms and converts to decimal degrees
    chars = set(':,°')
    if any((c in chars) for c in dms):
        delimiter = True
    else:
        delimiter = False
    
    if delimiter:
        if dms[0] == '-':
            if len(dms) == 9:
                negative = True
                dms_d = dms[1:3]
                dms_m = dms[4:6]
                dms_s = dms[7:9]
                
            else:
                negative = True
                dms_d = dms[1:4]
                dms_m = dms[5:7]
                dms_s = dms[8:10]
                
        else:
            if len(dms) == 8:
                negative = False
                dms_d = dms[0:2]
                dms_m = dms[3:5]
                dms_s = dms[6:8]
                
            else:
                negative = False
                dms_d = dms[0:3]
                dms_m = dms[4:6]
                dms_s = dms[7:9]
         
    else:
        if dms[0] == '-':
            if len(dms) == 7:
                negative = True
                dms_d = dms[1:3]
                dms_m = dms[3:5]
                dms_s = dms[5:7]

            else:
                negative = True
                dms_d = dms[1:4]
                dms_m = dms[4:6]
                dms_s = dms[6:8]
                
        else:
            if len(dms) == 6:
                negative = False
                dms_d = dms[0:2]
                dms_m = dms[2:4]
                dms_s = dms[4:6]
                
            else:
                negative = False
                dms_d = dms[0:3]
                dms_m = dms[3:5]
                dms_s = dms[5:7]
                
    dd = float(dms_d) + float(dms_m)/60 + float(dms_s)/(60*60)    
    
    if negative:
        dd = -dd
    
    return dd
#-----------------------------------------------------------------------------