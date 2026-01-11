import asyncio
import logging
import signal
import time
import math
import numpy as np
from common.redis_client import RedisClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

WORKER_ID = "lidar_worker"

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

shutdown_event = asyncio.Event()
redis = RedisClient(loop=loop, worker_id=WORKER_ID)

# Grid config
GRID_SIZE = 100
GRID_RESOLUTION = 0.5  # meters per cell
DIST_UNKNOWN_MM = 65535
DIST_MIN_MM = 200
DIST_MAX_MM = 10000

# Global state
occupancy_grid = np.zeros((GRID_SIZE, GRID_SIZE), dtype=np.uint8)
drone_pose = {"x": 0.0, "y": 0.0, "yaw": 0.0}
time_boot_us = 0


def world_to_grid(x: float, y: float):
    gx = int(x / GRID_RESOLUTION + GRID_SIZE // 2)
    gy = int(y / GRID_RESOLUTION + GRID_SIZE // 2)
    return gx, gy


def update_grid(x: float, y: float, yaw_deg: float,
                distances_mm: list[int], angle_offset_deg: int, angle_inc_deg: int):
    global occupancy_grid
    for i, dmm in enumerate(distances_mm):
        if dmm == DIST_UNKNOWN_MM or dmm < DIST_MIN_MM or dmm > DIST_MAX_MM:
            continue

        angle_deg = angle_offset_deg + i * angle_inc_deg
        angle_rad = math.radians(angle_deg + yaw_deg)

        dx = math.cos(angle_rad) * dmm / 1000.0
        dy = math.sin(angle_rad) * dmm / 1000.0

        ox = x + dx
        oy = y + dy

        gx, gy = world_to_grid(ox, oy)
        if 0 <= gx < GRID_SIZE and 0 <= gy < GRID_SIZE:
            occupancy_grid[gx, gy] = 255


async def publish_grid():
    """Publish occupancy grid to Redis and emit update event"""
    await redis.client.set("occupancy_grid", occupancy_grid.tobytes())
    await redis.publish("lidar_worker:occupancy_grid_updated", {"status": "updated", "timestamp": time.time()})
    logger.info(f"[{WORKER_ID}] Occupancy grid published")


# ---------------- Redis Event Listeners ---------------- #

@redis.listen("mission_manager:drone_pose_update")
async def handle_drone_pose_update(data):
    """Update drone pose from Mission Manager"""
    global drone_pose
    drone_pose["x"] = data.get("position", [0, 0, 0])[0]
    drone_pose["y"] = data.get("position", [0, 0, 0])[1]
    drone_pose["yaw"] = data.get("yaw", 0.0)
    logger.debug(f"[{WORKER_ID}] Pose updated: {drone_pose}")


@redis.listen("mission_manager:lidar_obstacle_distance")
async def handle_obstacle_distance(data):
    """Process OBSTACLE_DISTANCE message from drone"""
    global time_boot_us
    msg_time = data.get("time_usec", 0)
    if msg_time < time_boot_us:
        return

    distances = data.get("distances", [])
    angle_offset = data.get("angle_offset", 0)
    angle_inc = data.get("increment", 10)

    update_grid(drone_pose["x"], drone_pose["y"], drone_pose["yaw"],
                distances, angle_offset, angle_inc)
    await publish_grid()


@redis.listen("mission_manager:system_time")
async def handle_system_time(data):
    """Update time sync"""
    global time_boot_us
    time_boot_us = data.get("time_unix_usec", 0)


# ---------------- Worker Loops ---------------- #

async def heartbeat_loop():
    while not shutdown_event.is_set():
        try:
            await redis.heartbeat()
        except Exception as e:
            logger.error(f"[{WORKER_ID}] Heartbeat error: {e}")
        await asyncio.sleep(1)


async def main(loop):
    await redis.connect()

    startup_mode = await redis.get_startup_mode()
    logger.info(f"[{WORKER_ID}] Starting in {startup_mode} mode")

    tasks = [
        loop.create_task(heartbeat_loop()),
    ]

    await shutdown_event.wait()

    logger.info(f"[{WORKER_ID}] Shutting down...")
    for task in tasks:
        task.cancel()

    await redis.close()


def handle_shutdown():
    logger.info(f"[{WORKER_ID}] Shutdown signal received")
    shutdown_event.set()


def runner():
    # Handle SIGINT / SIGTERM
    # for sig in (signal.SIGINT, signal.SIGTERM):
    #     loop.add_signal_handler(sig, handle_shutdown)

    try:
        loop.run_until_complete(main(loop))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"[{WORKER_ID}] Fatal error: {e}")
    finally:
        handle_shutdown()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    runner()