from typing import Any, Dict, Optional

from app.database.db.session import AsyncSessionLocal
from app.database.models.audit_log import AuditLog
from app.src.logger import logger


async def audit_logs(
    action: str,
    resource_type: str,
    resource_id: Optional[str] = None,
    status: str = "success",
    actor_user_id: Optional[int] = None,
    organization_id: Optional[int] = None,
    meta_data: Optional[Dict[str, Any]] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    endpoint: Optional[str] = None,
):
    async with AsyncSessionLocal() as db:
        try:
            entry = AuditLog(
                actor_user_id=actor_user_id,
                organization_id=organization_id,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                status=status,
                meta_data=meta_data,
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )

            db.add(entry)
            await db.commit()

        except Exception as e:
            await db.rollback()

            log_context = {
                "action": action,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "status": status,
                "actor_user_id": actor_user_id,
                "organization_id": organization_id,
                "ip_address": ip_address,
                "endpoint": endpoint,
            }
            logger.error(
                "Audit log failed",
                extra={"error": str(e), **log_context},
            )

            raise