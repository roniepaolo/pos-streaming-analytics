from utils.csv_strategy import CSVStrategy
from utils.kafka_helper import KafkaHelper


def main():
    kh: KafkaHelper = KafkaHelper(source_strategy=CSVStrategy())
    kh.produce_source(
        topic="prod.stores",
        path="data/store.csv",
        header=["store_id", "name"],
        types=[int, str],
        key=["store_id"],
    )
    kh.produce_source(
        topic="prod.items",
        path="data/item.csv",
        header=["item_id", "name", "supplier_id", "safety_stock_quantity"],
        types=[int, str, int, int],
        key=["item_id"],
    )
    kh.produce_source(
        topic="prod.inventory.types",
        path="data/inventory_types.csv",
        header=["change_type_id", "change_type"],
        types=[int, str],
        key=["change_type_id"],
    )
    kh.produce_source(
        topic="prod.instore.inventory.transactions",
        path="data/instore_inventory_transactions.csv",
        header=[
            "trans_id",
            "item_id",
            "store_id",
            "date_time",
            "quantity",
            "change_type_id",
        ],
        types=[str, int, int, str, int, int],
        key=["trans_id", "item_id", "store_id"],
    )
    kh.produce_source(
        topic="prod.instore.inventory.snapshots",
        path="data/instore_inventory_snapshots.csv",
        header=[
            "item_id",
            "employee_id",
            "store_id",
            "date_time",
            "quantity",
        ],
        types=[int, int, int, str, int],
        key=["item_id", "store_id", "date_time"],
    )
    kh.produce_source(
        topic="prod.online.inventory.transactions",
        path="data/online_inventory_transactions.csv",
        header=[
            "trans_id",
            "item_id",
            "store_id",
            "date_time",
            "quantity",
            "change_type_id",
        ],
        types=[str, int, int, str, int, int],
        key=["trans_id", "item_id", "store_id"],
    )
    kh.produce_source(
        topic="prod.online.inventory.snapshots",
        path="data/online_inventory_snapshots.csv",
        header=[
            "item_id",
            "employee_id",
            "store_id",
            "date_time",
            "quantity",
        ],
        types=[int, int, int, str, int],
        key=["item_id", "store_id", "date_time"],
    )


if __name__ == "__main__":
    main()
