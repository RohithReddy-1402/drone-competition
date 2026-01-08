import time
import numpy as np
import matplotlib.pyplot as plt
from pymavlink import mavutil

# -----------------------------
# MAVLink connection (GCS port)
# -----------------------------
def runner():
    master = mavutil.mavlink_connection("udp:127.0.0.1:13551")
    master.wait_heartbeat()
    print("Connected to SITL")

    # request OBSTACLE_DISTANCE messages
    master.mav.request_data_stream_send(
        master.target_system,
        master.target_component,
        mavutil.mavlink.MAV_DATA_STREAM_ALL,
        10,  # 10 Hz
        1    # start
    )

    # -----------------------------
    # Matplotlib polar plot setup
    # -----------------------------
    plt.ion()
    fig = plt.figure(figsize=(6, 6))
    ax = fig.add_subplot(111, projection="polar")

    ax.set_theta_zero_location("N")   # 0° = forward
    ax.set_theta_direction(-1)        # clockwise (FRD)
    ax.set_rmax(8)                   # meters (adjust as needed)
    ax.set_rticks([1, 2, 3 ,4, 5, 6, 7, 8])
    ax.grid(True)

    scatter = ax.scatter([], [], s=4)

    while True:
        msg = master.recv_match(type="OBSTACLE_DISTANCE", blocking=True)
        if msg is None:
            continue

        distances_cm = np.array(msg.distances, dtype=np.float32)
        distances_m = distances_cm / 100.0

        # Angular resolution (supports increment_f = 0.1°)
        if msg.increment_f > 0:
            angle_inc = msg.increment_f
        else:
            angle_inc = msg.increment

        angles_deg = np.arange(len(distances_m)) * angle_inc + msg.angle_offset
        angles_rad = np.deg2rad(angles_deg)

        # Filter out max-range values
        max_range_m = msg.max_distance / 100.0
        valid = distances_m <= max_range_m

        angles_rad = angles_rad[valid]
        distances_m = distances_m[valid]
        
        print(len(distances_m))

        scatter.set_offsets(np.column_stack((angles_rad, distances_m)))
        ax.set_title("OBSTACLE_DISTANCE (Live from SITL)")

        plt.pause(0.01)

if __name__ == "__main__":
    runner()