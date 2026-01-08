import asyncio
import time
import logging
from pymavlink import mavutil
from common.redis_client import RedisClient

logger = logging.getLogger(__name__)

class MAVLinkManager:
    def __init__(self, redis: RedisClient, scout_uri: str, sprayer_uri: str):
        self.redis = redis

        self.scout = mavutil.mavlink_connection(scout_uri)
        self.sprayer = mavutil.mavlink_connection(sprayer_uri)
        
        self.scout.wait_heartbeat()
        logger.info("[MAVLink] Connected to scout")
        self.sprayer.wait_heartbeat()
        logger.info("[MAVLink] Connected to sprayer")
        
        self.drones = [self.scout, self.sprayer]

        # subscribe to all MAVLink messages
        for drone in self.drones:
            drone.mav.request_data_stream_send(
                drone.target_system,
                drone.target_component,
                mavutil.mavlink.MAV_DATA_STREAM_ALL,
                1,  # 1 Hz
                1   # start
            )
        logger.info(f"[MAVLink] Requested data stream from drones")

    async def listen(self):
        """Main MAVLink receive loop"""
        while True:
            await self._poll(self.scout, "scout")
            await self._poll(self.sprayer, "sprayer")
            await asyncio.sleep(0.01)

    async def _poll(self, link, drone_id):
        msg = link.recv_match(blocking=False)
        if not msg:
            return

        msg_type = msg.get_type()

        if msg_type == "LOCAL_POSITION_NED":
            await self._handle_local_position(msg, drone_id)

        elif msg_type == "ATTITUDE":
            await self._handle_attitude(msg, drone_id)

        elif msg_type == "SYSTEM_TIME":
            await self._handle_system_time(msg)

        elif msg_type == "OBSTACLE_DISTANCE" and drone_id == "scout":
            await self._handle_obstacle_distance(msg)

    async def _handle_local_position(self, msg, drone_id):
        payload = {
            "drone_id": drone_id,
            "position": [msg.x, msg.y, msg.z],
            "timestamp": time.time()
        }

        await self.redis.publish("mission_manager:drone_pose_update", payload)

    async def _handle_attitude(self, msg, drone_id):
        payload = {
            "drone_id": drone_id,
            "yaw": msg.yaw * 57.2958,  # rad → deg
            "timestamp": time.time()
        }

        await self.redis.publish("mission_manager:drone_attitude_update", payload)

    async def _handle_system_time(self, msg):
        await self.redis.publish("mission_manager:system_time", {
            "time_unix_usec": msg.time_unix_usec
        })

    async def _handle_obstacle_distance(self, msg):
        payload = {
            "time_usec": msg.time_usec,
            "distances": list(msg.distances),
            "angle_offset": msg.angle_offset,
            "increment": msg.increment
        }

        await self.redis.publish("mission_manager:lidar_obstacle_distance", payload)

    def send_waypoint(self, drone_id, x, y, z):
        link = self.scout if drone_id == "scout" else self.sprayer

        link.mav.set_position_target_local_ned_send(
            int(time.time() * 1e6),
            link.target_system,
            link.target_component,
            mavutil.mavlink.MAV_FRAME_LOCAL_NED,
            0b0000111111000111,  # position only
            x, y, z,
            0, 0, 0,
            0, 0, 0,
            0, 0
        )

    def halt(self, drone_id):
        link = self.scout if drone_id == "scout" else self.sprayer
        link.mav.command_long_send(
            link.target_system,
            link.target_component,
            mavutil.mavlink.MAV_CMD_NAV_LOITER_UNLIM,
            0, 0, 0, 0, 0, 0, 0
        )

# testing
def test():
    
    import os
    from dotenv import load_dotenv

    load_dotenv()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    redis = RedisClient(loop=loop)

    async def main():
        await redis.connect()

        mav_manager = MAVLinkManager(redis,
            scout_uri=os.environ.get("SCOUT_MAVLINK_UDP", "udp:localhost:13550"),
            sprayer_uri=os.environ.get("SPRAYER_MAVLINK_UDP", "udp:localhost:13560")
        )

        await mav_manager.listen()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Shutting down...")
          
if __name__ == "__main__":
    test()