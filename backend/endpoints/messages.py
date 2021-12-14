from utils.room_utils import get_room
from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from schema.message import Message, MessageRequest
from schema.response import ResponseModel
from starlette.responses import JSONResponse
from utils.centrifugo import Events, centrifugo_client
from utils.db import DataStorage
from bson.objectid import ObjectId


router = APIRouter()

MESSAGE_COLLECTION = "messages"

@router.post(
    "/org/{org_id}/rooms/{room_id}/sender/{sender_id}/messages",
    response_model=ResponseModel,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"detail": "room or sender not found"},
        424: {"detail": "ZC Core Failed"},
    },
)
async def send_message(
    org_id, room_id, sender_id, request:MessageRequest, background_tasks: BackgroundTasks
):
    """Creates and sends a message from one user to another.
    Registers a new document to the chats database collection.
    Returns the message info and document id if message is successfully created
    while publishing to all members of the room in the background
    Args:
        org_id (str): A unique identifier of an organisation
        request: A pydantic schema that defines the message request parameters
        room_id: A unique identifier of the room where the message is being sent to.
        sender_id: A unique identifier of the user sending the message
        background_tasks: A daemon thread for publishing centrifugo
    Sample request:
            {
            "text": "testing messages"
            }
    Returns:
        HTTP_201_CREATED {new message sent}:
        A dict containing data about the message that was created (response_output).
            {
                "room_id": "61b3fb328f945e698c7eb396",
                "message_id": "61696f43c4133ddga309dcf6",
                "text": "str",
                "files": "HTTP url(s)"
                "sender_id": "619ba4671a5f54782939d385"
            }
    Raises:
        HTTPException [404]: Sender not in room
        HTTPException [404]: Room does not exist
        HTTPException [424]: "message not sent"
    """
    DB = DataStorage(org_id)
    message_obj = Message(**request.dict(), org_id=org_id, room_id=room_id,
                            sender_id= sender_id)
    response = await DB.write(MESSAGE_COLLECTION, message_obj.dict())

    if response and response.get("status_code") is None:
        message_obj.message_id = response["data"]["object_id"]
        output_data = {
            "room_id": message_obj.room_id,
            "message_id": message_obj.message_id,
            "sender_id": message_obj.sender_id,
            "text": message_obj.text,
            "files": message_obj.files
        }
        background_tasks.add_task(
            centrifugo_client.publish, room_id, Events.MESSAGE_CREATE, output_data
        )  # publish to centrifugo in the background
        return JSONResponse(
            content=ResponseModel.success(data=output_data, message="new message sent"),
            status_code=status.HTTP_201_CREATED,
        )
    raise HTTPException(
            status_code=status.HTTP_424_FAILED_DEPENDENCY,
            detail={"Message not sent": response},
        )


@router.get(
    "/org/{org_id}/rooms/{room_id}/messages",
    response_model=ResponseModel,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"detail": "room or sender not found"},
        424: {"detail": "ZC Core Failed"},
    },
)
async def read_message(org_id: str, room_id: str):
    """Reads messages in the collection.

    Args:
        org_id (str): A unique identifier of an organisation
        request: A pydantic schema that defines the message request parameters
        room_id: A unique identifier of the room where the message is being sent to.

    Returns:
        HTTP_200_OK {messages retrieved}:
        A dict containing data about the messages in the collection based on the message schema (response_output).
            {
                "_id": "61b8caec78fb01b18fac1410",
                "created_at": "2021-12-14 16:40:43.302519",
                "files": [],
                "message_id": null,
                "org_id": "619ba4671a5f54782939d384",
                "reactions": [],
                "room_id": "619e28c31a5f54782939d59a",
                "saved_by": [],
                "sender_id": "61696f5ac4133ddaa309dcfe",
                "text": "testing messages",
                "threads": []
            }

    Raises:
        HTTP_404_FAILED_DEPENDENCY: Room does not exist
        HTTP_424_FAILED_DEPENDENCY: "No messages in collection"
    """
    
    DB = DataStorage(org_id)
    if org_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invalid Organization id",
        )

    room = await get_room(org_id=org_id, room_id=room_id)
    if not room:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Room does not exist"
        )

    try:
        messages = await DB.read(MESSAGE_COLLECTION, query={"org_id": org_id, "room_id": room_id})
        if messages:
            return JSONResponse(
                content=ResponseModel.success(
                    data=messages, message="messages retrieved"
                ),
                status_code=status.HTTP_200_OK,
            )
        raise HTTPException(
            status_code=status.HTTP_424_FAILED_DEPENDENCY,
            detail={"No messages in room": messages},
        )
    except Exception as e:
        raise e


@router.put(
    "/org/{org_id}/rooms/{room_id}/messages/{message_id}",
    response_model=ResponseModel,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"detail": "room or sender not found"},
        424: {"detail": "ZC Core Failed"},
    },
)
async def update_message(
    org_id: str, room_id: str, message_id: str, request: MessageRequest
):
    """Updates a message in the collection.

    Args:
        org_id (str): A unique identifier of an organisation
        request: A pydantic schema that defines the message request parameters
        room_id: A unique identifier of the room where the message is being sent to.
        message_id: A unique identifier of the message that is being updated.

    Returns:
        HTTP_200_OK {message updated}:
        A dict containing data about the message that was updated (response_output).
            {
                "_id": "61b8caec78fb01b18fac1410",
                "created_at": "2021-12-14 16:40:43.302519",
                "files": [],
                "message_id": null,
                "org_id": "619ba4671a5f54782939d384",
                "reactions": [],
                "room_id": "619e28c31a5f54782939d59a",
                "saved_by": [],
                "sender_id": "61696f5ac4133ddaa309dcfe",
                "text": "testing messages",
                "threads": []
            }

    Raises:
        HTTP_404_FAILED_DEPENDENCY: Room does not exist
        HTTP_424_FAILED_DEPENDENCY: "No messages in collection"
    """
    DB = DataStorage(org_id)
    if org_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Invalid Organization id",
        )

    room = await get_room(org_id=org_id, room_id=room_id)
    if not room:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Room does not exist"
        )

    x = await MESSAGE_COLLECTION.find_one({"_id": ObjectId(message_id)})
    if not x:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Message does not exist"
        )
        
    try:
        message = await DB.update(
            MESSAGE_COLLECTION,
            query={"org_id": org_id, "room_id": room_id, "message_id": message_id},
            data=request.dict(),
        )
        if message:
            return JSONResponse(
                content=ResponseModel.success(
                    data=message, message="message updated"
                ),
                status_code=status.HTTP_200_OK,
            )
        raise HTTPException(
            status_code=status.HTTP_424_FAILED_DEPENDENCY,
            detail={"No messages in collection": message},
        )
    except Exception as e:
        raise e