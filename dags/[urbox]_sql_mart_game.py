insert_data_mart_game = """
INSERT INTO data_mart.mart_game_updating
SELECT
             site_user_id,
             cart_detail_id,
             cart_id,
             transaction_id,
             thoi_gian_cong_luot_quay,
             ncart_id,
             gift_name,
              app_id,
             rn ,
             thoi_gian_tra_qua,
             reward_id,
             id_qua,
             ten_qua,
             gia_tri_qua,
             customer_rewards_id,
             game_id,
             is_test,
             so_luong_qua_set_up_config,
             ref_id,
             qua_luot_quay_test
from
    (

WITH qua_luot_quay AS
--     Quang anh confirm: game_id = 23 customer_reward.customer_id là phone, các trường hợp khác là site_user_id
         (SELECT case
                    when site_id = 1060 then cart.phone
                    else cart.site_user_id end                                                                     AS site_user_id,
                 cart_detail.id                                                                                    AS cart_detail_id,
                 cart.id                                                                                           AS cart_id,
                 cart.transaction_id                                                                               AS transaction_id,
                 cart.created                                                                                      AS thoi_gian_cong_luot_quay,
                 cart.ncart_id                                                                                     AS ncart_id,
                 gift_detail.title                                                                                 AS gift_name,
                 row_number() OVER (PARTITION BY cart.site_user_id, gift_detail.id ORDER BY cart_detail.id ASC)                    AS rn,
                 multiIf(gift_detail.id = 11025, 15, gift_detail.id = 12728, 20, gift_detail.id = 12917, 21,
                     gift_detail.id = 12987, 23, gift_detail.id = 13821, 24, gift_detail.id = 14050, 27, gift_detail.id = 14176, 29,
                         NULL)                                                                                     AS game_id,
                 cart.site_id                                                                                      as app_id,
                  case
                    when cart.site_id = 1060 and (cart.created + toIntervalHour(7)) > '2023-11-13 23:59:59' then 0
                    when cart.site_id = 1020 and gift_detail.id = 13821 and (cart.created + toIntervalHour(7)) > '2024-01-14 23:59:59' then 0
                    when cart.site_id = 1020 and gift_detail.id = 14050 and ((cart.created + toIntervalHour(7)) > '2024-02-06 23:59:59' and (cart.created + toIntervalHour(7)) <= '2024-02-25 23:59:59') then 0
                    when cart.site_id = 1020 and gift_detail.id = 14050 and ((cart.created + toIntervalHour(7)) > '2024-03-07 23:59:59' and (cart.created + toIntervalHour(7)) <= '2024-03-21 23:59:59') then 0
                    when cart.site_id = 1020 and gift_detail.id = 14176 and ((cart.created + toIntervalHour(7)) > '2024-03-13 23:59:59' and (cart.created + toIntervalHour(7)) <= '2024-03-21 23:59:59') then 0
                    else 1
                end AS qua_luot_quay_test
          FROM urgift.cart
                   INNER JOIN urgift.cart_detail ON cart.id = cart_detail.cart_id
                   INNER JOIN urgift.gift_detail ON gift_detail.id = cart_detail.gift_detail_id
          WHERE 1 = 1
-- Quang Anh ko quan tam đieu kien app: Quang Anh lấy theo id_quà để cộng lượt quay
            AND   (site_id in (127,1020, 1060))
            AND (cart_detail.status = 2)
            AND (cart_detail.pay_status = 2)
            AND (cart_detail.delivery NOT IN (4, 4011, 4012, 4021, 4022))
            and cart.status = 2 and cart.pay_status = 2 and cart.delivery <> 4
            AND (cart.created + toIntervalHour(7) <=
                makeDateTime(
                    YEAR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    MONTH(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    day(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    HOUR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    00, 00) - toIntervalHour(1))
            AND gift_detail.id in (12728, 12917, 12987, 13821, 14050, 14176)
--     loại lượt quay test
--             AND case
--                     when cart.site_id = 1060 then (cart.created + toIntervalHour(7)) > '2023-11-13 23:59:59'
--                     when cart.site_id = 1020 and gift_detail.id = 13821 then (cart.created + toIntervalHour(7)) > '2024-01-14 23:59:59'
--                     else 1 = 1 end
          ),
     qua_trung AS
         (SELECT customer_rewards.customer_id                                                                   AS site_user_id,
    row_number() OVER (PARTITION BY customer_rewards.customer_id, rewards.game_id ORDER BY customer_rewards.id ASC) AS rn,
                 cart.id                                                                                        AS ma_don_hang_urbox,
rewards.id                                                                                     AS reward_id,
                 rewards.gift_id                                                                                AS id_qua,
                 rewards.name                                                                                   AS ten_qua,
                 cart.money_total                                                                               AS gia_tri_qua,
                 toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh')                                  AS thoi_gian_tra_qua,
                 customer_rewards.transaction_id                                                                AS transaction_id,
                 multiIf(
                     (games.id = 20) AND (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') > '2023-10-11 06:00:00'), 0,
                     (games.id = 21) AND (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') > '2023-11-01 00:00:00'), 0,
                     (games.id = 23) AND  (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') > '2023-11-13 23:59:59'), 0,
                     (games.id = 24) AND  (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') > '2024-01-14 23:59:59'), 0,
                     (games.id = 27) AND  ((toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') > '2024-02-25 23:59:59') and (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') <= '2024-03-10 23:59:59')), 0,
                     (games.id = 27) AND  ((toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') > '2024-03-21 23:59:59') and (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') <= '2024-04-05 23:59:59')), 0,
                     (games.id = 29) AND  ((toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') > '2024-03-21 23:59:59') and (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') <= '2024-04-05 23:59:59')), 0,
                     lower(customer_rewards.customer_id) like '%test%', 1,
                         1)                                                                                     AS is_test,
                 customer_rewards.id                                                                            AS customer_rewards_id,
                 games.id                                                                                       AS game_id,
                 quantity_rewards.quantity                                                                      AS so_luong_qua_set_up_config,
                 customer_rewards.transaction_id                                                                AS ref_id
          FROM urgame.games
                   LEFT JOIN urgame.rewards ON rewards.game_id = games.id
                   LEFT JOIN urgame.quantity_rewards ON quantity_rewards.reward_id = rewards.id
                   INNER JOIN urgame.customer_rewards ON customer_rewards.reward_id = rewards.id
                   LEFT JOIN urgift.cart ON cart.transaction_id = customer_rewards.transaction_id and cart.transaction_id <> ''
          WHERE (1 = 1)
            and games.id in  (20, 21, 23, 24, 27, 29)
            AND (customer_rewards.created + toIntervalHour(7) <=
                makeDateTime(
                    YEAR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    MONTH(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    day(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    HOUR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    00, 00) - toIntervalHour(1))
            AND (customer_rewards.state IN (1, 3))
            AND (customer_rewards.id NOT IN (1058069, 1086796, 1099202))
            -- Danh sách Quang Anh gửi: qua trung bi timeout => lỗi game bị gián đoạn nhưng vẫn trả quà. Quang Anh update thông tin: Customer_reward.state = 1, game_turn.turn: cộng lượt quay, quantity_reward.recieved: giảm quà trúng
            AND (customer_rewards.id NOT IN (1440286,1440288,1440291,1440294,1440295,1440298,1440301,1440312,1440328,1440329,1440344,1440345,1440362,1440371,1440379,1440380,1440399,1440400,1440403,1440405,1440406,1440408,1440414,1440428,1440436,1440439,1440440,1440442,1440446,1440447,1440451,1443341,1443342,1443345,1443353,1443357,1443358,1443363,1443367,1443371,1443384,1443385,1443386,1443390,1443392,1443393,1443401,1443403,1443409,1443411,1443414,1443415,1443417,1443422,1443425,1443426,1443429,1443434,1443435,1443438,1443443,1443446,1443447,1443450,1443455,1443457,1443458,1443463,1443467,1443470,1443471,1443475,1443483,1443484,1443490,1443491,1443492,1443494,1443503,1443505,1443508,1443512,1443513,1443515,1443520,1443527,1443529,1443532,1443538,1443540,1443541,1443552,1443555,1443563,1443565,1443568,1443572,1443582,1443584,1443586,1443596,1443597,1443600,1443604,1443606,1443608,1443614,1443616,1443618,1443624,1443626,1443629,1443634,1443639,1443640,1443644,1443650,1443651,1443652,1443654,1443667,1443668,1443674,1443676,1443680,1443685,1443691,1443700,1443701,1443706,1443707,1443713,1443714,1443720,1443721,1443742,1443743,1443748,1443762,1443764,1443765,1443767,1443769,1443770,1443783,1443785,1443786,1443788,1443794,1443796,1443801,1443802,1443803,1443807,1443815,1443818,1443826,1443828,1443835,1443840,1443841,1443844,1443845,1443866,1443868,1443870,1443873,1443877,1443878,1443882,1443885,1443889,1443890,1443892,1443896,1443897,1443898))
          )
     ,
     cte3 AS
         (
         SELECT ifNull(qua_luot_quay.site_user_id,qua_trung.site_user_id) as site_user_id,
                 qua_luot_quay.cart_detail_id,
                 qua_luot_quay.cart_id,
                 qua_luot_quay.transaction_id,
                 qua_luot_quay.thoi_gian_cong_luot_quay,
                 qua_luot_quay.ncart_id,
                 qua_luot_quay.gift_name,
                 qua_luot_quay.app_id,
                 qua_luot_quay.rn,
                 qua_luot_quay.qua_luot_quay_test,
                 qua_trung.thoi_gian_tra_qua,
                 qua_trung.reward_id as reward_id,
                 qua_trung.id_qua,
                 qua_trung.ten_qua,
                 qua_trung.gia_tri_qua,
                 customer_rewards_id,
                 ifNull(qua_luot_quay.game_id,qua_trung.game_id) as game_id,
                 qua_trung.is_test as is_test,
                 qua_trung.so_luong_qua_set_up_config,
                 qua_trung.ref_id
          FROM qua_luot_quay
--               Nhieu trường hợp cộng lượt quay qua API nên không có dữ liệu trong bảng quà trúng
                   FULL OUTER JOIN qua_trung ON (qua_luot_quay.site_user_id = qua_trung.site_user_id) AND
                                          (qua_luot_quay.rn = qua_trung.rn and qua_luot_quay.game_id = qua_trung.game_id)

          )
SELECT
             site_user_id,
             cart_detail_id,
             cart_id,
             transaction_id,
             thoi_gian_cong_luot_quay,
             ncart_id,
             gift_name,
              app_id,
             rn ,
             thoi_gian_tra_qua,
             reward_id,
             id_qua,
             ten_qua,
             gia_tri_qua,
             customer_rewards_id,
             game_id,
             is_test,
             so_luong_qua_set_up_config,
             ref_id,
             qua_luot_quay_test
    from cte3
        )
        SETTINGS join_use_nulls = 0,
        join_algorithm = 'partial_merge'
"""
insert_data_mart_game_with_type_19 = """
INSERT INTO data_mart.mart_game_with_type_19_updating
SELECT
             site_user_id,
             cart_detail_id,
             cart_id,
             transaction_id,
             thoi_gian_cong_luot_quay,
             ncart_id,
             gift_name,
              app_id,
             rn ,
             thoi_gian_tra_qua,
             reward_id,
             id_qua,
             ten_qua,
             gia_tri_qua,
             customer_rewards_id,
             game_id,
             is_test,
             so_luong_qua_set_up_config,
             ref_id,
             qua_luot_quay_test
from
    (

WITH qua_luot_quay AS
--     Quang anh confirm: game_id = 23 customer_reward.customer_id là phone, các trường hợp khác là site_user_id
         (SELECT case
                    when site_id = 1060 then cart.phone
                    else cart.site_user_id end                                                                     AS site_user_id,
                 cart_detail.id                                                                                    AS cart_detail_id,
                 cart.id                                                                                           AS cart_id,
                 cart.transaction_id                                                                               AS transaction_id,
                 cart.created                                                                                      AS thoi_gian_cong_luot_quay,
                 cart.ncart_id                                                                                     AS ncart_id,
                 gift_detail.title                                                                                 AS gift_name,
                 row_number() OVER (PARTITION BY cart.site_user_id, gift_detail.id ORDER BY cart_detail.id ASC)                    AS rn,
                 multiIf(gift_detail.id = 12987, 23, NULL)                                                                                     AS game_id,
                 cart.site_id                                                                                      as app_id,
                  case
                    when cart.site_id = 1060 and (cart.created + toIntervalHour(7)) <= '2023-11-13 23:59:59' then 1
                    when cart.site_id = 127 then 1
                    else 0
                end AS qua_luot_quay_test
          FROM urgift.cart
                   INNER JOIN urgift.cart_detail ON cart.id = cart_detail.cart_id
                   INNER JOIN urgift.gift_detail ON gift_detail.id = cart_detail.gift_detail_id
          WHERE 1 = 1
-- Quang Anh ko quan tam đieu kien app: Quang Anh lấy theo id_quà để cộng lượt quay
            AND   (site_id in (127, 1060))
            AND (cart_detail.status = 2)
            AND (cart_detail.pay_status = 2)
            AND (cart_detail.delivery NOT IN (4, 4011, 4012, 4021, 4022))
            and cart.status = 2 and cart.pay_status = 2 and cart.delivery <> 4
            AND (cart.created + toIntervalHour(7) <=
                makeDateTime(
                    YEAR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    MONTH(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    day(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    HOUR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    00, 00) - toIntervalHour(1))
            AND gift_detail.id = 12987
--     loại lượt quay test
--             AND case
--                     when cart.site_id = 1060 then (cart.created + toIntervalHour(7)) > '2023-11-13 23:59:59'
--                     when cart.site_id = 1020 and gift_detail.id = 13821 then (cart.created + toIntervalHour(7)) > '2024-01-14 23:59:59'
--                     else 1 = 1 end
          ),
     qua_trung AS
         (SELECT customer_rewards.customer_id                                                                   AS site_user_id,
--     row_number() OVER (PARTITION BY customer_rewards.customer_id, rewards.game_id ORDER BY customer_rewards.id ASC) AS rn,
                 cart.id                                                                                        AS ma_don_hang_urbox,
rewards.id                                                                                     AS reward_id,
                 rewards.gift_id                                                                                AS id_qua,
                 rewards.name                                                                                   AS ten_qua,
                 cart.money_total                                                                               AS gia_tri_qua,
                 toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh')                                  AS thoi_gian_tra_qua,
                 customer_rewards.transaction_id                                                                AS transaction_id,
                 multiIf(
                     (games.id = 23) AND  (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') <= '2023-11-13 23:59:59'), 1,
                     lower(customer_rewards.customer_id) like '%test%', 1,
                         0)                                                                                     AS is_test,
                 customer_rewards.id                                                                            AS customer_rewards_id,
                 games.id                                                                                       AS game_id,
                 quantity_rewards.quantity                                                                      AS so_luong_qua_set_up_config,
                 customer_rewards.transaction_id                                                                AS ref_id
          FROM urgame.games
                   LEFT JOIN urgame.rewards ON rewards.game_id = games.id
                   LEFT JOIN urgame.quantity_rewards ON quantity_rewards.reward_id = rewards.id
                   INNER JOIN urgame.customer_rewards ON customer_rewards.reward_id = rewards.id
                   LEFT JOIN urgift.cart ON cart.transaction_id = customer_rewards.transaction_id and cart.transaction_id <> ''
          WHERE (1 = 1)
            and games.id =23
            AND (customer_rewards.created + toIntervalHour(7) <=
                makeDateTime(
                    YEAR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    MONTH(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    day(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    HOUR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                    00, 00) - toIntervalHour(1))
            AND (customer_rewards.state IN (1, 3))
            AND (customer_rewards.id NOT IN (1058069, 1086796, 1099202))
            -- Danh sách Quang Anh gửi: qua trung bi timeout => lỗi game bị gián đoạn nhưng vẫn trả quà. Quang Anh update thông tin: Customer_reward.state = 1, game_turn.turn: cộng lượt quay, quantity_reward.recieved: giảm quà trúng
            AND (customer_rewards.id NOT IN (1440286,1440288,1440291,1440294,1440295,1440298,1440301,1440312,1440328,1440329,1440344,1440345,1440362,1440371,1440379,1440380,1440399,1440400,1440403,1440405,1440406,1440408,1440414,1440428,1440436,1440439,1440440,1440442,1440446,1440447,1440451,1443341,1443342,1443345,1443353,1443357,1443358,1443363,1443367,1443371,1443384,1443385,1443386,1443390,1443392,1443393,1443401,1443403,1443409,1443411,1443414,1443415,1443417,1443422,1443425,1443426,1443429,1443434,1443435,1443438,1443443,1443446,1443447,1443450,1443455,1443457,1443458,1443463,1443467,1443470,1443471,1443475,1443483,1443484,1443490,1443491,1443492,1443494,1443503,1443505,1443508,1443512,1443513,1443515,1443520,1443527,1443529,1443532,1443538,1443540,1443541,1443552,1443555,1443563,1443565,1443568,1443572,1443582,1443584,1443586,1443596,1443597,1443600,1443604,1443606,1443608,1443614,1443616,1443618,1443624,1443626,1443629,1443634,1443639,1443640,1443644,1443650,1443651,1443652,1443654,1443667,1443668,1443674,1443676,1443680,1443685,1443691,1443700,1443701,1443706,1443707,1443713,1443714,1443720,1443721,1443742,1443743,1443748,1443762,1443764,1443765,1443767,1443769,1443770,1443783,1443785,1443786,1443788,1443794,1443796,1443801,1443802,1443803,1443807,1443815,1443818,1443826,1443828,1443835,1443840,1443841,1443844,1443845,1443866,1443868,1443870,1443873,1443877,1443878,1443882,1443885,1443889,1443890,1443892,1443896,1443897,1443898))

          UNION ALL

             select customer_rewards.customer_id                                  AS site_user_id,
                    null                                                          AS ma_don_hang_urbox,
                    customer_rewards.reward_id                                    AS reward_id,
                    rewards.gift_id                                               AS id_qua,
                    rewards.name                                                  AS ten_qua,
                    cart.money_total                                              AS gia_tri_qua,
                    toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') AS thoi_gian_tra_qua,
                    customer_rewards.transaction_id                               AS transaction_id,
                    multiIf(
                                (games.id = 23) AND
                                (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') <=
                                 '2023-11-13 23:59:59'), 1,
                                lower(customer_rewards.customer_id) like '%test%', 1,
                                0)                                                AS is_test,
                    customer_rewards.id                                           AS customer_rewards_id,
                    games.id                                                      AS game_id,
                    quantity_rewards.quantity                                     AS so_luong_qua_set_up_config,
                    customer_rewards.transaction_id                               AS ref_id


             FROM urgame.games
                      LEFT JOIN urgame.rewards ON rewards.game_id = games.id
                      LEFT JOIN urgame.quantity_rewards ON quantity_rewards.reward_id = rewards.id
                      INNER JOIN urgame.customer_rewards ON customer_rewards.reward_id = rewards.id
                      inner join cart_pay_new.cart on cart.transaction_id = customer_rewards.transaction_id
                      inner join (select distinct id, ncart_id, campaign_id, created_at
                                  from urcard.dim_tbl_unidentified_cards) dim_tbl_unidentified_cards
                                 on dim_tbl_unidentified_cards.ncart_id = cart.id
             where games.id = 23
               and customer_rewards.state = 4 -- lỗi rq game
               and cart.status = 1 -- call uc thất bại
          )
     ,
     cte3 AS
         (
         SELECT ifNull(qua_luot_quay.site_user_id,qua_trung.site_user_id) as site_user_id,
                 qua_luot_quay.cart_detail_id,
                 qua_luot_quay.cart_id,
                 qua_luot_quay.transaction_id,
                 qua_luot_quay.thoi_gian_cong_luot_quay,
                 qua_luot_quay.ncart_id,
                 qua_luot_quay.gift_name,
                 qua_luot_quay.app_id,
                 qua_luot_quay.rn,
                 qua_luot_quay.qua_luot_quay_test,
                 qua_trung.thoi_gian_tra_qua,
                 qua_trung.reward_id as reward_id,
                 qua_trung.id_qua,
                 qua_trung.ten_qua,
                 multiIf(qua_trung.gia_tri_qua is null, 0, qua_trung.gia_tri_qua) as gia_tri_qua,
                 customer_rewards_id,
                 ifNull(qua_luot_quay.game_id,qua_trung.game_id) as game_id,
                 qua_trung.is_test as is_test,
                 qua_trung.so_luong_qua_set_up_config,
                 qua_trung.ref_id
          FROM qua_luot_quay
--               Nhieu trường hợp cộng lượt quay qua API nên không có dữ liệu trong bảng quà trúng
                   FULL OUTER JOIN (select *,
                           row_number() OVER (PARTITION BY site_user_id, game_id ORDER BY customer_rewards_id ASC) AS rn
 from qua_trung) as qua_trung ON (qua_luot_quay.site_user_id = qua_trung.site_user_id) AND
                                          (qua_luot_quay.rn = qua_trung.rn and qua_luot_quay.game_id = qua_trung.game_id)

          )
SELECT
             site_user_id,
             cart_detail_id,
             cart_id,
             transaction_id,
             thoi_gian_cong_luot_quay,
             ncart_id,
             gift_name,
              app_id,
             rn ,
             thoi_gian_tra_qua,
             reward_id,
             id_qua,
             ten_qua,
             gia_tri_qua,
             customer_rewards_id,
             game_id,
             is_test,
             so_luong_qua_set_up_config,
             ref_id,
             qua_luot_quay_test
    from cte3

        )
        SETTINGS join_use_nulls = 0,
        join_algorithm = 'partial_merge';
"""
insert_data_mart_game_fec = """
INSERT INTO data_mart.mart_game_fec_updating
SELECT *
from (
    with qua_trung AS
             (SELECT customer_rewards.customer_id                                                                                    AS site_user_id,
                     row_number() OVER (PARTITION BY customer_rewards.customer_id, rewards.game_id ORDER BY customer_rewards.id ASC) AS rn,
                     cart.id                                                                                                         AS ma_don_hang_urbox,
                     rewards.id                                                                                                      AS reward_id,
                     rewards.gift_id                                                                                                 AS id_qua,
                     rewards.name                                                                                                    AS ten_qua,
                     cart.money_total                                                                                                AS gia_tri_qua,
                     toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh')                                                   AS thoi_gian_tra_qua,
                     customer_rewards.transaction_id                                                                                 AS transaction_id,
                     multiIf(
                                 (games.id = 25) AND
                                 (toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') <=
                                  '2024-01-25 23:59:59'),
                                 1,
                                 lower(customer_rewards.customer_id) like '%test%', 1,
                                 0)                                                                                                  AS is_test,
                     customer_rewards.id                                                                                             AS customer_rewards_id,
                     games.id                                                                                                        AS game_id,
                     quantity_rewards.quantity                                                                                       AS so_luong_qua_set_up_config,
                     customer_rewards.transaction_id                                                                                 AS ref_id
              FROM urgame.games
                       LEFT JOIN urgame.rewards ON rewards.game_id = games.id
                       LEFT JOIN urgame.quantity_rewards ON quantity_rewards.reward_id = rewards.id
                       INNER JOIN urgame.customer_rewards ON customer_rewards.reward_id = rewards.id
                       LEFT JOIN urgift.cart
                                 ON cart.transaction_id = customer_rewards.transaction_id and cart.transaction_id <> ''
              WHERE (1 = 1)
                and games.id = 25
                AND (customer_rewards.created + toIntervalHour(7) <=
                     makeDateTime(
                             YEAR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             MONTH(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             day(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             HOUR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             00, 00) - toIntervalHour(1))
                AND (customer_rewards.state IN (1, 3)))
            ,
         cte3 AS
             (SELECT ifNull(qua_luot_quay.site_user_id, qua_trung.site_user_id) as site_user_id,
                     qua_trung.transaction_id,
                     qua_luot_quay.thoi_gian_cong_luot_quay,
                     qua_trung.rn,
                     qua_trung.thoi_gian_tra_qua,
                     qua_trung.reward_id                                        as reward_id,
                     qua_trung.id_qua,
                     qua_trung.ten_qua,
                     qua_trung.gia_tri_qua,
                     customer_rewards_id,
                     ifNull(qua_luot_quay.game_id, qua_trung.game_id)           as game_id,
                     qua_trung.is_test                                          as is_test,
                     qua_trung.so_luong_qua_set_up_config,
                     qua_trung.ref_id,
                     qua_luot_quay.turns,
                     qua_luot_quay.state,
                     qua_luot_quay.ma_du_thuong
              FROM (select
    game_id,
                           game_codes.customer_id                                                                            as site_user_id,
                           game_codes.created                                                                     as thoi_gian_cong_luot_quay,
                           row_number() OVER (PARTITION BY site_user_id, game_id ORDER BY game_codes.created ASC) AS rn,
                           game_codes.turns as turns,
                           game_codes.state as state,
                           right(link,10) as ma_du_thuong
                    from urgame.game_codes
                    where game_id = 25
                  ) qua_luot_quay
--               Nhieu trường hợp cộng lượt quay qua API nên không có dữ liệu trong bảng quà trúng
                       FULL OUTER JOIN qua_trung ON (qua_luot_quay.site_user_id = qua_trung.site_user_id) AND
                                                    (qua_luot_quay.rn = qua_trung.rn and
                                                     qua_luot_quay.game_id = qua_trung.game_id))
    SELECT site_user_id,
           transaction_id,
           thoi_gian_cong_luot_quay,
           thoi_gian_tra_qua,
           reward_id,
           id_qua,
           ten_qua,
           gia_tri_qua,
           customer_rewards_id,
           game_id,
           multiIf(state = -2, 1, 0) as is_test,
           so_luong_qua_set_up_config,
           ref_id,
           turns,
           state,
           ma_du_thuong
    from cte3
    )
    SETTINGS join_use_nulls = 0
   , join_algorithm = 'partial_merge'
"""
insert_data_mart_game_scg = """
INSERT INTO data_mart.mart_game_scg_updating
with qua_trung AS
         (SELECT customer_rewards.customer_id                              AS site_user_id,
                 cart.id                                                   AS ma_don_hang_urbox,
                 rewards.id                                                AS reward_id,
                 rewards.gift_id                                           AS id_qua,
                 rewards.name                                              AS ten_qua,
                 cart.money_total                                          AS gia_tri_qua,
                 customer_rewards.created + toIntervalHour(7)              AS thoi_gian_tra_qua,
                 customer_rewards.transaction_id                           AS transaction_id,
                 multiIf(customer_rewards.state = 1, 1, 0)                 AS is_test,
                 customer_rewards.id                                       AS customer_rewards_id,
                 games.id                                                  AS game_id,
                 quantity_rewards.quantity                                 AS so_luong_qua_set_up_config,
                 customer_rewards.transaction_id                           AS ref_id,
                 customer_rewards.code_redeem                              AS ma_du_thuong,
                 customer_rewards.customer_id                              AS phone_num,
                 ___province.title                                         as thanh_pho,
                 JSON_VALUE(customer_rewards.metadata, '$.store.district') AS quan_huyen,
                 JSON_VALUE(customer_rewards.metadata, '$.store.name')     as ten_dai_ly

          FROM urgame.games
                   LEFT JOIN urgame.rewards ON rewards.game_id = games.id
                   LEFT JOIN urgame.quantity_rewards ON quantity_rewards.reward_id = rewards.id
                   INNER JOIN urgame.customer_rewards ON customer_rewards.reward_id = rewards.id
                   LEFT JOIN urgift.cart
                             ON cart.transaction_id = customer_rewards.transaction_id and cart.transaction_id <> ''
                   LEFT JOIN urgift.___province
                             on ___province.id = toInt32(JSON_VALUE(customer_rewards.metadata, '$.city_id'))
          WHERE (1 = 1)
            and games.id = 30
            AND (customer_rewards.created + toIntervalHour(7) <=
                 makeDateTime(
                         YEAR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                         MONTH(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                         day(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                         HOUR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                         00, 00) - toIntervalHour(1))
            AND (customer_rewards.state IN (1, 3))

          UNION ALL

          select customer_rewards.customer_id                              AS site_user_id,
                 null                                                      AS ma_don_hang_urbox,
                 customer_rewards.reward_id                                AS reward_id,
                 rewards.gift_id                                           AS id_qua,
                 rewards.name                                              AS ten_qua,
                 cart.money_total                                          AS gia_tri_qua,
                 customer_rewards.created + toIntervalHour(7)              AS thoi_gian_tra_qua,
                 customer_rewards.transaction_id                           AS transaction_id,
                 multiIf(customer_rewards.state = 1, 1, 0)                 AS is_test,
                 customer_rewards.id                                       AS customer_rewards_id,
                 games.id                                                  AS game_id,
                 quantity_rewards.quantity                                 AS so_luong_qua_set_up_config,
                 customer_rewards.transaction_id                           AS ref_id,
                 customer_rewards.code_redeem                              AS ma_du_thuong,
                 customer_rewards.customer_id                              AS phone_num,
                 ___province.title                                         as thanh_pho,
                 JSON_VALUE(customer_rewards.metadata, '$.store.district') AS quan_huyen,
                 JSON_VALUE(customer_rewards.metadata, '$.store.name')     as ten_dai_ly


          FROM urgame.games
                   LEFT JOIN urgame.rewards ON rewards.game_id = games.id
                   LEFT JOIN urgame.quantity_rewards ON quantity_rewards.reward_id = rewards.id
                   INNER JOIN urgame.customer_rewards ON customer_rewards.reward_id = rewards.id
                   inner join cart_pay_new.cart on cart.transaction_id = customer_rewards.transaction_id
                   inner join (select distinct id, ncart_id, campaign_id, created_at
                               from urcard.dim_tbl_unidentified_cards) dim_tbl_unidentified_cards
                              on dim_tbl_unidentified_cards.ncart_id = cart.id
                   LEFT JOIN urgift.___province
                             on ___province.id = toInt32(JSON_VALUE(customer_rewards.metadata, '$.city_id'))
          where games.id = 30
            and customer_rewards.state = 4 -- lỗi rq game
            and cart.status = 1 -- call uc thất bại
         )
SELECT qua_trung.site_user_id                           as site_user_id,
       qua_trung.transaction_id                         as transaction_id,
       qua_luot_quay.thoi_gian_cong_luot_quay           as thoi_gian_tao_ma_du_thuong,
       qua_trung.thoi_gian_tra_qua                      as thoi_gian_tra_qua,
       qua_trung.reward_id                              as reward_id,
       qua_trung.id_qua                                 as id_qua,
       qua_trung.ten_qua                                as ten_qua,
       qua_trung.gia_tri_qua                            as gia_tri_qua,
       customer_rewards_id,
       ifNull(qua_luot_quay.game_id, qua_trung.game_id) as game_id,
       qua_trung.is_test                                as is_test,
       qua_trung.so_luong_qua_set_up_config             as so_luong_qua_set_up_config,
       qua_trung.ref_id                                 as ref_id,
       qua_luot_quay.turns                              as turns,
       qua_luot_quay.state                              as state,
       qua_luot_quay.ma_du_thuong                       as ma_du_thuong,
       qua_trung.thanh_pho                              as thanh_pho,
       qua_trung.quan_huyen                             as quan_huyen,
       qua_trung.ten_dai_ly                             as ten_dai_ly
FROM (select game_id,
             game_codes.created as thoi_gian_cong_luot_quay,
             game_codes.turns   as turns,
             game_codes.state   as state,
             game_codes.code    as ma_du_thuong
      from urgame.game_codes
      where game_id = 30
        and state in (1, 3)
    ) qua_luot_quay
         FULL OUTER JOIN qua_trung ON qua_luot_quay.ma_du_thuong = qua_trung.ma_du_thuong

    SETTINGS join_use_nulls = 0
   , join_algorithm = 'partial_merge';"""
insert_data_mart_game_dulux_31 = """INSERT INTO data_mart.mart_game_dulux_31_updating
SELECT site_user_id,
       cart_detail_id,
       cart_id,
       transaction_id,
       thoi_gian_cong_luot_quay,
       ncart_id,
       gift_name,
       app_id,
       rn,
       thoi_gian_tra_qua,
       reward_id,
       id_qua,
       ten_qua,
       gia_tri_qua,
       customer_rewards_id,
       game_id,
       is_test,
       so_luong_qua_set_up_config,
       ref_id,
       qua_luot_quay_test,
       region
from (
    WITH qua_luot_quay AS
             (SELECT case
                         when site_id = 1060 then cart.phone
                         else cart.site_user_id end                                                                 AS site_user_id,
                     cart_detail.id                                                                                 AS cart_detail_id,
                     cart.id                                                                                        AS cart_id,
                     cart.transaction_id                                                                            AS transaction_id,
                     cart.created + toIntervalHour(7)                                                               AS thoi_gian_cong_luot_quay,
                     cart.ncart_id                                                                                  AS ncart_id,
                     gift_detail.title                                                                              AS gift_name,
                     row_number() OVER (PARTITION BY cart.site_user_id, gift_detail.id ORDER BY cart_detail.id ASC) AS rn,
                     multiIf(gift_detail.id = 15097, 31, NULL)                                                      AS game_id,
                     cart.site_id                                                                                   as app_id,
                     case
                         when thoi_gian_cong_luot_quay > '2024-07-03 23:59:59' then 0
                         else 1
                         end                                                                                        AS qua_luot_quay_test
              FROM urgift.cart
                       INNER JOIN urgift.cart_detail ON cart.id = cart_detail.cart_id
                       INNER JOIN urgift.gift_detail ON gift_detail.id = cart_detail.gift_detail_id
              WHERE 1 = 1
                AND (site_id in (127, 1020, 1060))
                AND (cart_detail.status = 2)
                AND (cart_detail.pay_status = 2)
                AND (cart_detail.delivery NOT IN (4, 4011, 4012, 4021, 4022))
                and cart.status = 2
                and cart.pay_status = 2
                and cart.delivery <> 4
                AND (cart.created + toIntervalHour(7) <=
                     makeDateTime(
                             YEAR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             MONTH(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             day(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             HOUR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             00, 00) - toIntervalHour(1))
                AND gift_detail.id = 15097),
         qua_trung AS
             (SELECT customer_rewards.customer_id                                                                                    AS site_user_id,
--                      row_number() OVER (PARTITION BY customer_rewards.customer_id, rewards.game_id ORDER BY customer_rewards.id ASC) AS rn,
                     cart.id                                                                                                         AS ma_don_hang_urbox,
                     rewards.id                                                                                                      AS reward_id,
                     rewards.gift_id                                                                                                 AS id_qua,
                     rewards.name                                                                                                    AS ten_qua,
                     cart.money_total                                                                                                AS gia_tri_qua,
                     customer_rewards.created + toIntervalHour(7)                                                                    AS thoi_gian_tra_qua,
                     customer_rewards.transaction_id                                                                                 AS transaction_id,
                     multiIf(
                                 (games.id = 31) AND ((thoi_gian_tra_qua <= '2024-10-15 23:59:59') and
                                                      (thoi_gian_tra_qua > '2024-07-03 23:59:59')), 0,
                                 lower(customer_rewards.customer_id) like '%test%', 1,
                                 1)                                                                                                  AS is_test,
                     customer_rewards.id                                                                                             AS customer_rewards_id,
                     games.id                                                                                                        AS game_id,
                     quantity_rewards.quantity                                                                                       AS so_luong_qua_set_up_config,
                     customer_rewards.transaction_id                                                                                 AS ref_id,
                     customer_rewards.region as region
              FROM urgame.games
                       LEFT JOIN urgame.rewards ON rewards.game_id = games.id
                       LEFT JOIN urgame.quantity_rewards ON quantity_rewards.reward_id = rewards.id
                       INNER JOIN urgame.customer_rewards ON customer_rewards.reward_id = rewards.id
                       LEFT JOIN urgift.cart
                                 ON cart.transaction_id = customer_rewards.transaction_id and cart.transaction_id <> ''
              WHERE (1 = 1)
                and games.id = 31
                AND (customer_rewards.created + toIntervalHour(7) <=
                     makeDateTime(
                             YEAR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             MONTH(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             day(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             HOUR(toDateTime64(now(), 6, 'Asia/Ho_Chi_Minh')),
                             00, 00) - toIntervalHour(1))
                AND (customer_rewards.state IN (1, 3))
                AND (customer_rewards.id NOT IN (1058069, 1086796, 1099202))

              UNION ALL

             select customer_rewards.customer_id                                  AS site_user_id,
                    null                                                          AS ma_don_hang_urbox,
                    customer_rewards.reward_id                                    AS reward_id,
                    rewards.gift_id                                               AS id_qua,
                    rewards.name                                                  AS ten_qua,
                    cart.money_total                                              AS gia_tri_qua,
                    toDateTime64(customer_rewards.created, 6, 'Asia/Ho_Chi_Minh') AS thoi_gian_tra_qua,
                    customer_rewards.transaction_id                               AS transaction_id,
                    multiIf(
                                 (games.id = 31) AND ((thoi_gian_tra_qua <= '2024-10-15 23:59:59') and
                                                      (thoi_gian_tra_qua > '2024-07-03 23:59:59')), 0,
                                 lower(customer_rewards.customer_id) like '%test%', 1,
                                 1)                                                                                                  AS is_test,
                    customer_rewards.id                                           AS customer_rewards_id,
                    games.id                                                      AS game_id,
                    quantity_rewards.quantity                                     AS so_luong_qua_set_up_config,
                    customer_rewards.transaction_id                               AS ref_id,
                    customer_rewards.region as region


             FROM urgame.games
                      LEFT JOIN urgame.rewards ON rewards.game_id = games.id
                      LEFT JOIN urgame.quantity_rewards ON quantity_rewards.reward_id = rewards.id
                      INNER JOIN urgame.customer_rewards ON customer_rewards.reward_id = rewards.id
                      inner join cart_pay_new.cart on cart.transaction_id = customer_rewards.transaction_id
                      inner join (select distinct id, ncart_id, campaign_id, created_at
                                  from urcard.dim_tbl_unidentified_cards) dim_tbl_unidentified_cards
                                 on dim_tbl_unidentified_cards.ncart_id = cart.id
             where games.id = 31
               and customer_rewards.state = 4 -- lỗi rq game
               and cart.status = 1 -- call uc thất bại
              )
            ,
         cte3 AS
             (SELECT ifNull(qua_luot_quay.site_user_id, qua_trung.site_user_id) as site_user_id,
                     qua_luot_quay.cart_detail_id,
                     qua_luot_quay.cart_id,
                     qua_luot_quay.transaction_id,
                     qua_luot_quay.thoi_gian_cong_luot_quay,
                     qua_luot_quay.ncart_id,
                     qua_luot_quay.gift_name,
                     qua_luot_quay.app_id,
                     qua_luot_quay.rn,
                     qua_luot_quay.qua_luot_quay_test,
                     qua_trung.thoi_gian_tra_qua,
                     qua_trung.reward_id                                        as reward_id,
                     qua_trung.id_qua,
                     qua_trung.ten_qua,
                     qua_trung.gia_tri_qua,
                     customer_rewards_id,
                     ifNull(qua_luot_quay.game_id, qua_trung.game_id)           as game_id,
                     qua_trung.is_test                                          as is_test,
                     qua_trung.so_luong_qua_set_up_config,
                     qua_trung.ref_id,
                     qua_trung.region
              FROM qua_luot_quay
--               Nhieu trường hợp cộng lượt quay qua API nên không có dữ liệu trong bảng quà trúng
                      FULL OUTER JOIN (select *,
                           row_number() OVER (PARTITION BY site_user_id, game_id ORDER BY customer_rewards_id ASC) AS rn
 from qua_trung) as qua_trung ON (qua_luot_quay.site_user_id = qua_trung.site_user_id) AND
                                          (qua_luot_quay.rn = qua_trung.rn and qua_luot_quay.game_id = qua_trung.game_id)
              )
    SELECT site_user_id,
           cart_detail_id,
           cart_id,
           transaction_id,
           thoi_gian_cong_luot_quay,
           ncart_id,
           gift_name,
           app_id,
           rn,
           thoi_gian_tra_qua,
           reward_id,
           id_qua,
           ten_qua,
           gia_tri_qua,
           customer_rewards_id,
           game_id,
           is_test,
           so_luong_qua_set_up_config,
           ref_id,
           qua_luot_quay_test,
           region
    from cte3
    )
    SETTINGS join_use_nulls = 0
   , join_algorithm = 'partial_merge';"""
