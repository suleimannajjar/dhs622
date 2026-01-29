from telethon.tl.types.messages import ChatFull
from telethon.tl.patched import Message as TelegramMessage

def extract_data_from_message_object(message: TelegramMessage) -> dict:
    message_dict = {
        "channel_id": message.to_dict()["peer_id"]["channel_id"],
        "message_id": message.to_dict()["id"],
        "message_datetime": message.to_dict()["date"],
        "message_views": message.to_dict()["views"],
        "message_forwards": message.to_dict()["forwards"],
        "message_text": message.to_dict()["message"],
        "forwardee_channel_id": None,
        "forwardee_message_id": None,
        "message_is_forward": False,
        "api_response": message.to_json(),
    }

    if message.fwd_from is not None:
        message_dict["message_is_forward"] = True
        if message.to_dict()["fwd_from"]["from_id"] is not None:
            if "channel_id" in message.to_dict()["fwd_from"]["from_id"].keys():
                message_dict["forwardee_channel_id"] = message.to_dict()["fwd_from"][
                    "from_id"
                ]["channel_id"]
                message_dict["forwardee_message_id"] = message.to_dict()["fwd_from"][
                    "channel_post"
                ]

    return message_dict

def extract_data_dictionary_from_channel_object(
    channel_object: ChatFull, channel_name: str
) -> dict:
    return {
        "channel_name": channel_name,
        "channel_id": channel_object.to_dict()["full_chat"]["id"],
        "channel_title": channel_object.to_dict()["chats"][0]["title"],
        "num_subscribers": channel_object.to_dict()["full_chat"]["participants_count"],
        "channel_bio": channel_object.to_dict()["full_chat"]["about"],
        "channel_birthdate": channel_object.to_dict()["chats"][0]["date"],
        "api_response": channel_object.to_json(),
    }