import sqlload as sl

shop = sl.source(
    "shop", connection="shop_api", table_name="{table}", write_strategy="merge"
)
api = sl.http(shop, auth=sl.bearer(), retries=5)


@shop.table(
    description="Orders, one row per order.",
    unique_key="id",
    cursor_column="updated_at",
    cursor_start="2026-01-01T00:00:00Z",
    columns={"updated_at": "timestamp_tz"},
    unnest="all",
)
def orders(ctx):
    return api.paginate(
        "/orders",
        params={"updated_after": ctx.cursor_value},
        paginator=sl.next_link("$.next"),
        select="$.items",
    )


@shop.table(description="Payments, one row per payment.", unique_key="id")
def payments(ctx):
    order_ids = api.paginate(
        "/orders", paginator=sl.next_link("$.next"), select="$.items[*].id"
    )
    return api.fan_out(
        "/orders/{id}/payments",
        over=order_ids,
        select="$.items",
        on_status={404: "skip"},
    )
