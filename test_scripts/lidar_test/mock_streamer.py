from pymavlink import mavutil
import numpy as np
import time

# -------------------------------
# MAVLink connection
# -------------------------------
master = mavutil.mavlink_connection("udp:127.0.0.1:13550",
    source_system=1,
    source_component=196)
master.wait_heartbeat()
print("Connected to ArduPilot SITL")

# -------------------------------
# OBSTACLE_DISTANCE parameters
# -------------------------------
NUM_READINGS = 72
ANGLE_INCREMENT = 5
MIN_DISTANCE_CM = 20
MAX_DISTANCE_CM = 2500

# -------------------------------
# Simulated RPLidar S2L scan
# Replace this with real UART data later
# -------------------------------
def get_fake_rplidar_scan():
    """
    Returns 72 distance values in cm.
    Simulates a wall at 5 meters in front.
    """
    # distances = np.full(NUM_READINGS, MAX_DISTANCE_CM, dtype=np.uint16)
    distances = [300] * 50 + [2500] * 22
    # Obstacle between -15° and +15°
    # for i in range(69, 72):
    #     distances[i] = 200
    # for i in range(0, 4):
    #     distances[i] = 200

    # return distances.tolist()
    return distances

# -------------------------------
# Main loop
# -------------------------------
while True:
    distances = get_fake_rplidar_scan()

    master.mav.obstacle_distance_send(
        time_usec=int(time.time() * 1e6),
        sensor_type=mavutil.mavlink.MAV_DISTANCE_SENSOR_LASER,
        distances=distances,
        increment=ANGLE_INCREMENT,
        min_distance=MIN_DISTANCE_CM,
        max_distance=MAX_DISTANCE_CM,
        increment_f=0.0,
        angle_offset=0.0,
        frame=mavutil.mavlink.MAV_FRAME_BODY_FRD
    )

    time.sleep(0.1)
