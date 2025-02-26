import logging
from app.entities.agent_data import AgentData
from app.entities.processed_agent_data import ProcessedAgentData

def process_agent_data(agent_data: AgentData) -> ProcessedAgentData:
    """
    Analyzes accelerometer data and classifies the road surface condition.

    Conditions (example):
      - If the Y-axis value is ≥ 2.5, it is considered a "Severe Pothole".
      - If the Y-axis value is between 1.5 and 2.5, it is classified as a "Pothole".
      - If the Z-axis value is < -0.7, it is classified as "Deep Uneven".
      - If the Z-axis value is between -0.7 and -0.3, it is classified as a "Slight Dip".
      - If the Z-axis value is > 0.7, it is classified as a "High Bump".
      - In all other cases, the road surface is considered "Smooth".
    """
    # Extract accelerometer readings
    acc = agent_data.accelerometer

    # Define threshold constants
    SEVERE_Y_THRESHOLD = 2.5
    POTHOLE_Y_THRESHOLD = 1.5
    DEEP_UNEVEN_Z_THRESHOLD = -0.7
    SLIGHT_DIP_Z_THRESHOLD_LOW = -0.7
    SLIGHT_DIP_Z_THRESHOLD_HIGH = -0.3
    HIGH_BUMP_Z_THRESHOLD = 0.7

    # Default road condition is "Smooth"
    road_state = "Smooth"

    logging.debug("Processing accelerometer data: y=%s, z=%s", acc.y, acc.z)

    if acc.y >= SEVERE_Y_THRESHOLD:
        road_state = "Severe Pothole"
    elif POTHOLE_Y_THRESHOLD <= acc.y < SEVERE_Y_THRESHOLD:
        road_state = "Pothole"
    elif acc.z < DEEP_UNEVEN_Z_THRESHOLD:
        road_state = "Deep Uneven"
    elif SLIGHT_DIP_Z_THRESHOLD_LOW <= acc.z < SLIGHT_DIP_Z_THRESHOLD_HIGH:
        road_state = "Slight Dip"
    elif acc.z > HIGH_BUMP_Z_THRESHOLD:
        road_state = "High Bump"

    logging.info("Classified road condition as: %s", road_state)

    processed_data = ProcessedAgentData(road_state=road_state, agent_data=agent_data)
    return processed_data
