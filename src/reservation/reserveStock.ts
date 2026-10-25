import { PoolClient } from 'pg';

export interface OrderItem {
  sku: string;
  qty: number;
  unit_price: number;
}

export interface NormalizedOrder {
  channel: 'WEBSITE' | 'NYKAA' | 'FLIPKART' | 'BLINKIT';
  external_order_id: string;
  event_time: string; // ISO 8601
  items: OrderItem[];
  total_amount: number;
  shipping_pincode: string;
}

export type ReservationResult =
  | {
      success: true;
      orderId: string;
      reservedItems: Array<{ sku: string; reservedQty: number; availableAfter: number }>;
    }
  | {
      success: false;
      reason: 'INSUFFICIENT_STOCK' | 'SKU_NOT_FOUND' | 'DUPLICATE_ORDER';
      failedSku?: string;
      available?: number;
      requested?: number;
      errorMessage?: string;
    };

/**
 * Executes atomic inventory reservation in PostgreSQL for an incoming multi-item order.
 * 
 * Concurrency & Safety Guarantees:
 * 1. ACID Isolation: Runs inside a READ COMMITTED transaction with row-level locks.
 * 2. Deadlock Prevention: Sorts SKUs lexicographically so concurrent transactions acquire locks in identical order.
 * 3. Strict Available Balance Guard: (physical_qty - reserved_qty) >= requested_qty.
 * 4. Immutable Audit Trail: Appends balance-after and reference ID to `inventory_ledger`.
 * 5. Idempotent Order Persistence: Unique constraint on (channel, external_order_id).
 */
export async function reserveStockForOrder(
  client: PoolClient,
  order: NormalizedOrder
): Promise<ReservationResult> {
  await client.query('BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED');

  try {
    // Step 1: Prevent deadlocks by enforcing deterministic lock acquisition order across SKUs
    const sortedItems = [...order.items].sort((a, b) => a.sku.localeCompare(b.sku));
    const reservedItems: Array<{ sku: string; reservedQty: number; availableAfter: number }> = [];

    // Step 2: Lock and evaluate each SKU row sequentially
    for (const item of sortedItems) {
      const lockRes = await client.query(
        `SELECT sku, physical_qty, reserved_qty, (physical_qty - reserved_qty) AS available_qty
         FROM inventory 
         WHERE sku = $1 
         FOR UPDATE`,
        [item.sku]
      );

      if (lockRes.rows.length === 0) {
        await client.query('ROLLBACK');
        return {
          success: false,
          reason: 'SKU_NOT_FOUND',
          failedSku: item.sku,
          errorMessage: `SKU ${item.sku} does not exist in master catalog.`
        };
      }

      const current = lockRes.rows[0];

      // Step 3: Concurrency Guard - Reject if insufficient stock
      if (current.available_qty < item.qty) {
        await client.query('ROLLBACK');

        // Persist reservation failure outside the aborted transaction for ops visibility
        await client.query(
          `INSERT INTO orders 
            (channel, external_order_id, channel_event_time, status, total_amount, shipping_pincode, last_error_message)
           VALUES ($1, $2, $3, 'RESERVATION_FAILED', $4, $5, $6)
           ON CONFLICT (channel, external_order_id) DO NOTHING`,
          [
            order.channel,
            order.external_order_id,
            order.event_time,
            order.total_amount,
            order.shipping_pincode,
            `OOS on SKU ${item.sku}: Requested ${item.qty}, Available ${current.available_qty}`
          ]
        );

        return {
          success: false,
          reason: 'INSUFFICIENT_STOCK',
          failedSku: item.sku,
          available: current.available_qty,
          requested: item.qty
        };
      }

      // Step 4: Increment reserved count atomically
      const updateRes = await client.query(
        `UPDATE inventory 
         SET reserved_qty = reserved_qty + $1,
             updated_at = NOW(),
             version = version + 1
         WHERE sku = $2
         RETURNING physical_qty, reserved_qty, (physical_qty - reserved_qty) AS available_qty`,
        [item.qty, item.sku]
      );

      const updated = updateRes.rows[0];

      // Step 5: Append immutable entry to the Inventory Ledger (Audit Trail)
      await client.query(
        `INSERT INTO inventory_ledger 
          (sku, delta_qty, physical_after, reserved_after, available_after, reason, reference_id, channel)
         VALUES ($1, $2, $3, $4, $5, 'ORDER_RESERVED', $6, $7)`,
        [
          item.sku,
          -item.qty,
          updated.physical_qty,
          updated.reserved_qty,
          updated.available_qty,
          order.external_order_id,
          order.channel
        ]
      );

      reservedItems.push({
        sku: item.sku,
        reservedQty: item.qty,
        availableAfter: updated.available_qty
      });
    }

    // Step 6: Create confirmed order record with PENDING ERP sync status
    const orderInsertRes = await client.query(
      `INSERT INTO orders 
        (channel, external_order_id, channel_event_time, status, total_amount, shipping_pincode, erp_sync_status)
       VALUES ($1, $2, $3, 'CONFIRMED', $4, $5, 'PENDING')
       RETURNING id`,
      [
        order.channel,
        order.external_order_id,
        order.event_time,
        order.total_amount,
        order.shipping_pincode
      ]
    );

    const orderId = orderInsertRes.rows[0].id;

    // Step 7: Insert line items
    for (const item of sortedItems) {
      await client.query(
        `INSERT INTO order_items (order_id, sku, qty, unit_price)
         VALUES ($1, $2, $3, $4)`,
        [orderId, item.sku, item.qty, item.unit_price]
      );
    }

    // Commit transaction: locks released, order confirmed, ledger committed
    await client.query('COMMIT');

    return {
      success: true,
      orderId,
      reservedItems
    };
  } catch (error: any) {
    await client.query('ROLLBACK');

    // Handle unique constraint conflict (duplicate order retry from channel)
    if (error.code === '23505') {
      return {
        success: false,
        reason: 'DUPLICATE_ORDER',
        errorMessage: `Duplicate order for channel ${order.channel} and ID ${order.external_order_id}`
      };
    }

    throw error;
  }
}
