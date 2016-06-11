from decimal import Decimal

import pytest
import sqlalchemy as sa

from sqlalchemy_utils.aggregates import aggregated


@pytest.fixture
def Product(Base):
    class Product(Base):
        __tablename__ = 'product'
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.Unicode(255))
        price = sa.Column(sa.Numeric)

        catalog_id = sa.Column(sa.Integer, sa.ForeignKey('car_catalog.id'))
    return Product


@pytest.fixture
def Catalog(Base):
    class Catalog(Base):
        __tablename__ = 'catalog'
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.Unicode(255))
        type = sa.Column(sa.Unicode(255))

        __mapper_args__ = {
            'polymorphic_on': type
        }

    return Catalog


@pytest.fixture
def CostumeCatalog(Catalog):
    class CostumeCatalog(Catalog):
        __tablename__ = 'costume_catalog'
        id = sa.Column(
            sa.Integer, sa.ForeignKey(Catalog.id), primary_key=True
        )

        __mapper_args__ = {
            'polymorphic_identity': 'costumes',
        }
    return CostumeCatalog


@pytest.fixture
def CarCatalog(Catalog, Product):
    class CarCatalog(Catalog):
        __tablename__ = 'car_catalog'
        id = sa.Column(
            sa.Integer, sa.ForeignKey(Catalog.id), primary_key=True
        )
        model = sa.Column(sa.Unicode(255), default='')
        products = sa.orm.relationship('Product', backref='catalog')

        @aggregated('products', sa.Column(sa.Numeric, default=0))
        def net_worth(self):
            return sa.func.sum(Product.price)

        __mapper_args__ = {
            'polymorphic_identity': 'cars',
        }
    return CarCatalog


@pytest.fixture
def init_models(Product, Catalog, CostumeCatalog, CarCatalog):
    pass


@pytest.mark.usefixtures('postgresql_dsn')
class TestLazyEvaluatedSelectExpressionsForAggregates(object):

    def test_joined_inheritance(self, session, CarCatalog, Product):
        catalog = CarCatalog(
            name=u'Some catalog',
            model=u'Model 1'
        )
        session.add(catalog)
        session.commit()

        catalog2 = CarCatalog(
            name=u'Another catalog',
            model=u'Model 2'
        )
        session.add(catalog2)
        session.commit()

        product = Product(
            name=u'Some product',
            price=Decimal('1000'),
            catalog=catalog
        )
        session.add(product)
        session.commit()

        product2 = Product(
            name=u'Another product',
            price=Decimal('2000'),
            catalog=catalog2
        )
        session.add(product2)
        session.commit()

        catalogs = session.query(CarCatalog).order_by('name').all()
        assert catalogs[0].net_worth == Decimal('2000')
        assert catalogs[1].net_worth == Decimal('1000')
        
