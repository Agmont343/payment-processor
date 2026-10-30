from fastapi import APIRouter, Depends

from app.api.deps import require_api_key
from app.api.v1.payments.create.endpoint import router as create_router
from app.api.v1.payments.get_by_id.endpoint import router as get_by_id_router

router = APIRouter(
    prefix="/api/v1",
    dependencies=[Depends(require_api_key)],
    tags=["payments"],
)
router.include_router(create_router)
router.include_router(get_by_id_router)
