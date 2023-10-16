import codecs
# -*- coding: UTF-8 -*-
 
import serial
import modbus_tk.defines as cst
from modbus_tk import modbus_rtu
import threading
import struct
import paho.mqtt.client as mqtt
import json
import time
import schedule 


#master = modbus_rtu.RtuMaster(serial.Serial(port='COM18', baudrate=9600, bytesize=8, parity="N", stopbits=1, xonxoff=0))
master = modbus_rtu.RtuMaster(serial.Serial(port='/dev/ttyO1', baudrate=9600, bytesize=8, parity="N", stopbits=1, xonxoff=0))
master.set_timeout(5.0)
master.set_verbose(True)


def modbus_connection():
    global master
    #try:
        #master = modbus_rtu.RtuMaster(serial.Serial(port='COM18', baudrate=9600, bytesize=8, parity="N", stopbits=1, xonxoff=0))
    master = modbus_rtu.RtuMaster(serial.Serial(port='/dev/ttyO1', baudrate=9600, bytesize=8, parity="N", stopbits=1, xonxoff=0))
    master.set_timeout(5.0)
    master.set_verbose(True)
    #except:
        #pass

def Current_ms():
    time_stamp_s = int(time.time()) # 轉成時間戳
    d = time_stamp_s % 60
    time_stamp_set = time_stamp_s - d
    time_stamp_ms = time_stamp_set * 1000
    
    return time_stamp_ms

def int2float(a,b):
    f=0
    try:
        z0=hex(a)[2:].zfill(4)
        z1=hex(b)[2:].zfill(4)
        z=z1+z0
        f=struct.unpack('!f', bytes.fromhex(z))[0]
    except BaseException as e:
        print(e)
    return f

def int2long(a,b):
    f=0
    try:
        z0=hex(a)[2:].zfill(4)
        z1=hex(b)[2:].zfill(4)
        z=z1+z0
        f=struct.unpack('!L', bytes.fromhex(z))[0]
    except BaseException as e:
        print(e)
    return f

def GatData():
    try:
        #1暫態體積流率(m3/hr)
        flow_VolumFlow = master.execute(1, cst.READ_HOLDING_REGISTERS, 1, 2)
        flow_VolumFlow_T = round(int2float(flow_VolumFlow[1], flow_VolumFlow[0]),1)

        #2暫態熱量(GJ/hr)
        flow_EnergyFlow = master.execute(1, cst.READ_HOLDING_REGISTERS, 3, 2)
        flow_EnergyFlow_T = round(int2float(flow_EnergyFlow[1], flow_EnergyFlow[0]),1)

        #3流體速度(GJ/hr)
        flow_flowrate = master.execute(1, cst.READ_HOLDING_REGISTERS, 5, 2)
        flow_flowrate_T = round(int2float(flow_flowrate[1], flow_flowrate[0]),1)

        #4正累積熱量(GJ)
        flow_POSEnergy = master.execute(1, cst.READ_HOLDING_REGISTERS, 121, 2)
        flow_POSEnergy_T = round(int2float(flow_POSEnergy[1], flow_POSEnergy[0]),2)

        #5負累積熱量(GJ)
        flow_NEGEnergy = master.execute(1, cst.READ_HOLDING_REGISTERS, 123, 2)
        flow_NEGEnergy_T = round(int2float(flow_NEGEnergy[1], flow_NEGEnergy[0]),2)

        #6淨累積熱量(GJ)
        flow_TotalEnergy = master.execute(1, cst.READ_HOLDING_REGISTERS, 119, 2)
        flow_TotalEnergy_T = round(int2float(flow_TotalEnergy[1], flow_TotalEnergy[0]),2)

        #7正累積流量
        flow_POSVolume = master.execute(1, cst.READ_HOLDING_REGISTERS, 115, 2)
        flow_POSVolume_T = round(int2float(flow_POSVolume[1], flow_POSVolume[0]),2)

        #8負累積流量
        flow_NEGVolume = master.execute(1, cst.READ_HOLDING_REGISTERS, 117, 2)
        flow_MoonVolume_T = round(int2float(flow_NEGVolume[1], flow_NEGVolume[0]),2)

        #9淨累積流量
        flow_TotalVolume = master.execute(1, cst.READ_HOLDING_REGISTERS, 113, 2)
        flow_TotalVolume_T = round(int2float(flow_TotalVolume[1], flow_TotalVolume[0]),2)
    
        #供水溫度(degC)
        flow_temp_in = master.execute(1, cst.READ_HOLDING_REGISTERS, 33, 2)
        flow_temp_Supply = round(int2float(flow_temp_in[1], flow_temp_in[0]),1)

        #回水溫度(degC)
        flow_temp_out = master.execute(1, cst.READ_HOLDING_REGISTERS, 35, 2)
        flow_temp_Return = round(int2float(flow_temp_out[1], flow_temp_out[0]),1)

        return (flow_VolumFlow_T, 
                flow_EnergyFlow_T, 
                flow_flowrate_T, 
                flow_POSEnergy_T, 
                flow_NEGEnergy_T, 
                flow_TotalEnergy_T, 
                flow_POSVolume_T, 
                flow_MoonVolume_T, 
                flow_TotalVolume_T,
                flow_temp_Supply,
                flow_temp_Return)
    except:
        pass




def Publish_UFM():
    try:
        UFM_Data = GatData()
        client = mqtt.Client()
        client.on_connect
        client.username_pw_set('HzpJuNmQs0Zahu76dvBW','XXX')
        client.connect('thingsboard.cloud', 1883, 60)
        TimeStamp = Current_ms()
        
        payload_iaq = {"ts": TimeStamp,
                       "values":{"FM_VolumFlow":UFM_Data[0],
                                 "FM_EnergyFlow":UFM_Data[1], 
                                 "FM_flowrate":UFM_Data[2], 
                                 "FM_POSEnergy":UFM_Data[3], 
                                 "FM_NEGEnergy":UFM_Data[4], 
                                 "FM_TotalEnergy":UFM_Data[5],
                                 "FM_POSVolume":UFM_Data[6], 
                                 "FM_MoonVolume":UFM_Data[7],
                                 "FM_TotalVolume":UFM_Data[8],
                                 "FM_temp_Supply":UFM_Data[9],
                                 "FM_temp_Return":UFM_Data[10]}}
        
        print (client.publish("v1/devices/me/telemetry", json.dumps(payload_iaq)))
        time.sleep(5)
    except:
        modbus_connection()


schedule.every(5).seconds.do(Publish_UFM)

if __name__ == '__main__':
    while True:
        schedule.run_pending()
        time.sleep(1)
