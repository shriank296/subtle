from sqlalchemy.exc import SQLAlchemyError

def sqlalchemy_error_handler(request: Request, exc: SQLAlchemyError) -> JSONResponse:
    """Catch-all handler for unexpected SQLAlchemy exceptions."""
    logger.error(
        "Unhandled SQLAlchemy error",
        exc_info=exc,
        extra={"path": request.url.path},
    )

    http_status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    body = ErrorResponse(
        timestamp=datetime.now(timezone.utc),
        status=http_status_code,
        title="Database Error",
        errors=[
            ErrorDetail(
                detail="An unexpected database error occurred. Please contact support if the issue persists."
            )
        ],
        path=request.url.path,
    ).model_dump()

    return JSONResponse(jsonable_encoder(body), status_code=http_status_code)