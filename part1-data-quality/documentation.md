# Summary of Data Quality Issues Detected
## 1. Missing Critical Fields
•	client_id missing in 2,908 rows
🔥 Critical issue — breaks attribution, session stitching, user-journey reconstruction.
•	referrer missing in 37,165 rows (March files)
Expected partly, but unusually high → possible GTM mis-fire or overwritten header.
•	event_data missing in 49,117 rows (98.3%)
→ For ecommerce events, this is a major failure because:
o	Add-to-cart should contain product_id, price
o	Checkout_started should contain cart-level details
o	Checkout_completed should contain order_id, revenue
This explains why revenue looked wrong — the payload that stores revenue was empty.

## 2. No Purchase / Revenue Events Found
Your dataset contains:
•	page_viewed: 45,974
•	product_added_to_cart: 2,307
•	checkout_started: 836
•	checkout_completed: 294
✔️ But ZERO revenue or purchase events are present.
📌 This is abnormal. checkout_completed events normally include a purchase payload (order_id + revenue).
Your checkout_completed rows have no event_data, meaning revenue = NULL everywhere.
→ This is the root cause of the dashboard revenue drop.

## 3. Duplicate Events Only 1 duplicate row detected .✔️ Good, not a major integrity issue.

## 4. Timestamps , 0 malformed timestamps. ✔️ ISO format consistent.

## 5. Abnormally High Missing event_data in All Event Types-systematic upstream problem likely
Possible root causes:
1.	GTM: Broken dataLayer push – payload object missing
2.	GTM: Wrong variable path (undefined object)
3.	Backend: Event stream schema changed mid-period
4.	Ingestion bug: event_data being discarded due to invalid JSON
5.	ETL: Event parser truncating nested fields

