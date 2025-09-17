from app.services import DataManager


def bootstrap():
    config = {
        'data_validation_enabled': True,
        'export_formats': ['csv', 'json', 'parquet']
    }
    dm = DataManager(config)
    return dm


if __name__ == '__main__':
    dm = bootstrap()
    print(f"DataManager ready. Symbols: {len(dm.symbols_list)}")

