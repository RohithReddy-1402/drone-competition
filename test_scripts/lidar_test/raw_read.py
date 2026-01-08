import time
import numpy as np
import matplotlib.pyplot as plt
from rplidar import RPLidar

# ==============================
# CONFIG
# ==============================
LIDAR_PORT = "COM5"          # change to your port
LIDAR_BAUD = 1000000          # RPLidar S2 / S2L
ANGLE_RES = 0.1              # degrees
NUM_BINS = int(360 / ANGLE_RES)

MIN_CM = 20
MAX_CM = 1800                # 18 meters

PLOT_RATE_HZ = 10
PLOT_PERIOD = 1.0 / PLOT_RATE_HZ

# ==============================
# Matplotlib setup
# ==============================
plt.ion()
fig = plt.figure(figsize=(6, 6))
ax = fig.add_subplot(111, projection="polar")

ax.set_theta_zero_location("N")   # 0° = forward
ax.set_theta_direction(-1)        # clockwise
ax.set_rmax(7)
ax.set_rticks([1, 2, 3, 4, 5, 6, 7])
ax.grid(True)

scatter = ax.scatter([], [], s=4)
ax.set_title("RPLidar S2 / S2L – Live Scan")

# ==============================
# RPLidar setup
# ==============================
lidar = RPLidar(LIDAR_PORT, baudrate=LIDAR_BAUD)
lidar.start_motor()
time.sleep(2)

# distance buffer (shared)
distances = np.full(NUM_BINS, MAX_CM, dtype=np.uint16)
last_plot = time.time()

# ==============================
# Main loop
# ==============================
try:
    # for meas in lidar.iter_measurments(max_buf_meas=7000):
    #     _, _, angle_deg, dist_mm = meas

    #     if dist_mm <= 0:
    #         continue

    #     dist_cm = int(dist_mm / 10)
    #     if not (MIN_CM <= dist_cm <= MAX_CM):
    #         continue

    #     idx = int(angle_deg / ANGLE_RES) % NUM_BINS
    #     distances[idx] = min(distances[idx], dist_cm)

    #     now = time.time()
    #     if now - last_plot >= PLOT_PERIOD:
    #         # Prepare plot data
    #         valid = distances < MAX_CM
    #         angles_deg = np.arange(NUM_BINS) * ANGLE_RES
    #         angles_rad = np.deg2rad(angles_deg[valid])
    #         ranges_m = distances[valid] / 100.0

    #         scatter.set_offsets(np.column_stack((angles_rad, ranges_m)))
    #         plt.pause(0.001)

    #         # reset buffer for next frame
    #         distances.fill(MAX_CM)
    #         last_plot = now

    for scan in lidar.iter_scans(max_buf_meas=7000):
        # reset buffer for this scan
        distances = np.full(NUM_BINS, MAX_CM, dtype=np.uint16)

        for (_, angle_deg, dist_mm) in scan:
            if dist_mm <= 0:
                continue

            dist_cm = int(dist_mm / 10)
            if dist_cm < MIN_CM or dist_cm > MAX_CM:
                continue

            idx = int(angle_deg / ANGLE_RES) % NUM_BINS
            distances[idx] = min(distances[idx], dist_cm)

        # Prepare plot data
        valid = distances < MAX_CM
        angles_deg = np.arange(NUM_BINS) * ANGLE_RES
        angles_rad = np.deg2rad(angles_deg[valid])
        ranges_m = distances[valid] / 100.0

        scatter.set_offsets(np.column_stack((angles_rad, ranges_m)))
        plt.pause(0.001)

except KeyboardInterrupt:
    print("Stopping...")

finally:
    lidar.stop()
    lidar.stop_motor()
    lidar.disconnect()
