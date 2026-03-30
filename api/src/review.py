"""
Opportunity review API endpoints.

Provides POST and GET endpoints for recording analyst review decisions
(consider/ignore) on opportunity flats.
"""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from typing import Optional

from db.src.write_read_database import OrthancDB

logger = logging.getLogger(__name__)
router = APIRouter()


class ReviewRequest(BaseModel):
    decision: str
    reason: Optional[str] = None

    @field_validator("decision")
    @classmethod
    def validate_decision(cls, v):
        if v not in ("consider", "ignore"):
            raise ValueError("decision must be 'consider' or 'ignore'")
        return v


@router.post("/{flat_id}")
async def post_review(flat_id: str, review: ReviewRequest):
    """
    Record a review decision for a flat.
    """
    with OrthancDB() as db:
        success = db.add_review(flat_id, review.decision, review.reason)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to save review")

    return {"success": True, "flat_id": flat_id, "decision": review.decision}


@router.get("/{flat_id}")
async def get_review(flat_id: str):
    """
    Get the most recent review for a flat, plus full history.
    """
    with OrthancDB() as db:
        latest = db.get_review(flat_id)
        history = db.get_reviews_for_flat(flat_id)

    if not latest:
        raise HTTPException(
            status_code=404, detail=f"No review found for flat {flat_id}"
        )

    return {"success": True, "review": latest, "history": history}
