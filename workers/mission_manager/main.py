import asyncio
import logging
import os
import signal
import time
from typing import Dict, Any
from dotenv import load_dotenv

load_dotenv()

from common.redis_client import RedisClient
from workers.mission_manager.mavlink_manager import MAVLinkManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

WORKER_ID = "mission_manager"

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

shutdown_event = asyncio.Event()
redis = RedisClient(loop=loop, worker_id=WORKER_ID)

mission_state: Dict[str, Any] = {
    "system_mode": "NORMAL",
    "active_drones": {},
    "pending_plans": {},
}

@redis.listen("event:planned_waypoint")
async def handle_planned_waypoint(data):
    """
    Waypoint computed by Path Planner.
    """
    drone_id = data.get("drone_id")
    waypoint = data.get("waypoint")

    logger.info(f"[MissionManager] Waypoint received for {drone_id}: {waypoint}")

    # TODO: validate drone state
    # TODO: issue MAVLink command to drone
    await send_waypoint_to_drone(drone_id, waypoint)

@redis.listen("event:no_safe_path")
async def handle_no_safe_path(data):
    """
    Planner reports no valid path.
    """
    drone_id = data.get("drone_id")

    logger.warning(f"[MissionManager] No safe path for {drone_id}")

    # TODO: hover / wait / abort logic

@redis.listen("event:occupancy_grid_updated")
async def handle_grid_update(data):
    """
    Notification that grid has changed.
    """
    logger.debug("[MissionManager] Occupancy grid updated")
    # TODO: decide whether to request replan

@redis.listen("event:crop_detected")
async def handle_crop_detected(data):
    """
    Crop detection event from Camera Worker.
    """
    logger.info(f"[MissionManager] Crop detected: {data}")

    # TODO: clustering logic
    # TODO: request path planner to plan sprayer path

@redis.listen("event:system_mode")
async def handle_system_mode(data):
    """
    Supervisor updates system-wide mode.
    """
    mode = data.get("mode", "NORMAL")
    mission_state["system_mode"] = mode

    logger.warning(f"[MissionManager] System mode set to {mode}")

    # TODO: enforce recovery behavior

async def send_waypoint_to_drone(drone_id: str, waypoint: Dict[str, Any]):
    """
    Placeholder for MAVLink waypoint command.
    """
    logger.info(f"[MissionManager] Sending waypoint to {drone_id}: {waypoint}")


async def halt_drone(drone_id: str):
    """
    Placeholder for HALT / HOVER command.
    """
    logger.warning(f"[MissionManager] Halting drone {drone_id}")
    # TODO: MAVLink SET_MODE / LOITER

async def heartbeat_loop():
    while not shutdown_event.is_set():
        try:
            await redis.heartbeat()
        except Exception as e:
            logger.error(f"[MissionManager] Heartbeat error: {e}")
        await asyncio.sleep(1)

async def main(loop):
    await redis.connect()

    startup_mode = await redis.get_startup_mode()
    logger.info(f"[MissionManager] Starting in {startup_mode} mode")

    mav_manager = MAVLinkManager(
        redis,
        scout_uri=os.environ.get("SCOUT_MAVLINK_UDP", "udp:localhost:13550"),
        sprayer_uri=os.environ.get("SPRAYER_MAVLINK_UDP", "udp:localhost:13560")
    )

    tasks = [
        loop.create_task(heartbeat_loop()),
        loop.create_task(mav_manager.listen()),
    ]

    await shutdown_event.wait()

    logger.info("[MissionManager] Shutting down...")
    for task in tasks:
        task.cancel()

    await redis.close()

def handle_shutdown():
    logger.info("[MissionManager] Shutdown signal received")
    shutdown_event.set()

def runner():
    # Signal handlers intentionally commented for Windows compatibility
    # for sig in (signal.SIGINT, signal.SIGTERM):
    #     loop.add_signal_handler(sig, handle_shutdown)

    try:
        loop.run_until_complete(main(loop))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"[MissionManager] Fatal error: {e}")
    finally:
        handle_shutdown()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    runner()
