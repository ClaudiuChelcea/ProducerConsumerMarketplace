"""
Marketplace
"""
import json
import time
import unittest
from threading import Lock, RLock
import logging
from logging.handlers import RotatingFileHandler
from collections import defaultdict
import heapq

class Marketplace:
    # pylint: disable=too-many-instance-attributes
    # pylint: disable-msg=too-many-locals
    """
    Marketplace
    """

    def __init__(self, queue_size_per_producer):
        self.q_size = queue_size_per_producer
        self.prod_q = {}
        self.carts = {}
        self.p_id, self.c_id = 0, 0
        self.p_id_lock, self.c_id_lock = Lock(), Lock()
        self.p_locks = defaultdict(RLock)
        self.prod_queue = defaultdict(list)
        self.prod_locks = {}

        self.logger = logging.getLogger('my_logger')
        self.logger.setLevel(logging.INFO)
        handler = RotatingFileHandler("marketplace.log", maxBytes=1024 * 512, backupCount=20)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        formatter.converter = time.gmtime
        self.logger.addHandler(handler)

    def register_producer(self):
        """
        Register producer.

        Returns:
            p_id_str (str): The id.
        """
        with self.p_id_lock:
            p_id_str = f"prod{self.p_id}"
            self.prod_q[p_id_str], self.p_locks[p_id_str] = 0, RLock()
            self.p_id += 1
            self.logger.info("Registered producer: %s", p_id_str)
        return p_id_str

    def new_cart(self):
        """
        Create cart.

        Returns:
            int: id.
        """
        with self.c_id_lock:
            cart_id = self.c_id
            self.carts[cart_id] = []
            self.c_id += 1
            self.logger.info("New cart created: %s", cart_id)
        return cart_id

    def add_product_to_cart(self, c_id, prod, p_id):
        """ Adds a product in the cart """
        self.carts[c_id].append({"product": prod, "producer_id": p_id})
        self.logger.info("Product added to cart: %s, %s", c_id, prod)

    def add_to_cart(self, c_id, prod):
        """
        Add to the cart.

        Args:
            int: id
            Product: prod

        Returns:
            bool: Status.
        """
        if c_id not in self.carts or not self.prod_queue[prod]:
            self.logger.info("Cart or Product not available: %s, %s", c_id, prod)
            return False

        if prod not in self.prod_locks:
            self.prod_locks[prod] = RLock()

        with self.prod_locks[prod]:
            if not self.prod_queue[prod]:
                self.logger.info("Product not available: %s, %s", c_id, prod)
                return False

            _, p_id = heapq.heappop(self.prod_queue[prod])
            self.add_product_to_cart(c_id, prod, p_id)
        return True

    def publish(self, p_id, prod):
        """
        Publish product

        Args:
            p_id (str): The id
            prod (str): The name

        Returns:
            bool: Status
        """
        with self.p_locks[p_id]:
            q_size = self.prod_q[p_id]
            if q_size == self.q_size:
                self.logger.info("Publish failed: %s, %s", p_id, prod)
                return False

            heapq.heappush(self.prod_queue[prod], (q_size, p_id))
            self.prod_q[p_id] += 1
            self.logger.info("Published product: %s, %s", p_id, prod)
        return True

    def _find_and_remove(self, cart, product):
        for i, item in enumerate(cart):
            if item['product'] == product:
                producer_id = item['producer_id']
                heapq.heappush(self.prod_queue[product], (self.prod_q[producer_id], producer_id))
                del cart[i]
                return True
        return False

    def remove_from_cart(self, cart_id, product):
        """
        Remove product

        Args:
            cart_id (int): ID
            product (str): Name

        Returns:
            bool: Status
        """
        if cart_id not in self.carts:
            self.logger.error("Cart %s does not exist.", cart_id)
            return False

        cart = self.carts[cart_id]
        removed = self._find_and_remove(cart, product)

        if removed:
            self.logger.info("Product %s removed from cart %s.", product, cart_id)
        else:
            self.logger.error("Product %s not found in cart %s.", product, cart_id)

        return removed

    def place_order(self, cart_id):
        """
        Remove product

        Args:
            cart_id (int): ID

        Returns:
            bool: Status
        """
        self.logger.info("Entered place_order(%s)!", cart_id)
        if cart_id not in self.carts:
            self.logger.info("Finished place_order(%s): Cart doesn't exist!", cart_id)
            return None

        cart_list = self.carts[cart_id]
        result = [cart_element["product"] for cart_element in cart_list]

        for cart_element in cart_list:
            producer_id = cart_element["producer_id"]
            with self.p_locks[producer_id]:
                self.prod_q[producer_id] -= 1

        self.carts[cart_id] = []
        self.logger.info("Finished place_order(%s): Order placed: %s!", cart_id, result)
        return result

    def get_cart(self, cart_id):
        """
        Get the cart.

        Args:
            cart_id (int): cart ID

        Returns:
            list: list of products in the cart, or None if cart doesn't exist.
        """
        if cart_id not in self.carts:
            self.logger.info("Cart does not exist: %s", cart_id)
            return None

        cart_list = self.carts[cart_id]
        result = [cart_element["product"] for cart_element in cart_list]
        self.logger.info("Cart retrieved: %s", cart_id)
        return result


class TestMarketplace(unittest.TestCase):
    # pylint: disable=too-many-instance-attributes
    # pylint: disable-msg=too-many-locals
    """
    Unit tests for the Marketplace class.
    """

    def test_init(self):
        """
        Test case for the __init__ method of the Marketplace class.
        """
        market = Marketplace(10)
        self.assertEqual(market.q_size, 10)
        self.assertEqual(market.prod_q, {})
        self.assertEqual(market.carts, {})
        self.assertEqual(market.p_id, 0)
        self.assertEqual(market.c_id, 0)
        self.assertEqual(market.p_locks, {})
        self.assertEqual(market.prod_locks, {})

    def test_register_producer(self):
        """
        Test case for the register_producer method of the Marketplace class.
        """
        market = Marketplace(10)
        p_id_str = market.register_producer()
        self.assertEqual(p_id_str, "prod0")
        self.assertEqual(market.prod_q, {"prod0": 0})
        self.assertEqual(len(market.p_locks), 1)
        self.assertEqual(len(market.prod_locks), 0)

    def test_publish(self):
        """
        Test case for the publish method of the Marketplace class.
        """
        market = Marketplace(10)
        p_id_str = market.register_producer()
        prod = "product"
        self.assertTrue(market.publish(p_id_str, prod))
        self.assertEqual(market.prod_q, {"prod0": 1})

    def test_new_cart(self):
        """
        Test case for the new_cart method of the Marketplace class.
        """
        market = Marketplace(10)
        c_id = market.new_cart()
        self.assertEqual(c_id, 0)
        self.assertEqual(market.carts, {0: []})

    def test_add_to_cart(self):
        """
        Test case for the add_to_cart method of the Marketplace class.
        """
        # Create a new marketplace
        market = Marketplace(10)

        # Create a cart and register a producer
        c_id = market.new_cart()
        p_id_str = market.register_producer()

        # Attempt to add a non-existent product to the cart
        prod = "product"
        self.assertFalse(market.add_to_cart(c_id, prod))

        # Publish the product and add it to the cart
        market.publish(p_id_str, prod)
        self.assertTrue(market.add_to_cart(c_id, prod))

        # Attempt to add the same product to the cart again
        self.assertFalse(market.add_to_cart(c_id, prod))

        # Attempt to add a product that doesn't exist
        self.assertFalse(market.add_to_cart(c_id, "non_existent_product"))

        # Attempt to add to an invalid cart
        self.assertFalse(market.add_to_cart(-1, prod))

        # Check that the product was added to the cart and the producer list
        self.assertEqual(market.carts, {0: [{"product": prod, "producer_id": "prod0"}]})

    def test_remove_from_cart(self):
        """
        Test case for the remove_from_cart method of the Marketplace class.
        """
        market = Marketplace(10)
        c_id = market.new_cart()
        p_id_str = market.register_producer()
        prod = "product"
        market.publish(p_id_str, prod)
        market.add_to_cart(c_id, prod)

        # Attempt to remove a non-existent product from the cart
        self.assertFalse(market.remove_from_cart(c_id, "non_existent_product"))

        # Remove a product from the cart
        self.assertTrue(market.remove_from_cart(c_id, prod))
        self.assertEqual(market.carts, {0: []})

    def test_place_order(self):
        """
        Test case for the place_order method of the Marketplace class.
        """
        market = Marketplace(10)
        c_id = market.new_cart()
        p_id_str = market.register_producer()
        prod = "product"
        market.publish(p_id_str, prod)
        market.add_to_cart(c_id, prod)

        # Attempt to place an order with a non-existent cart
        self.assertIsNone(market.place_order(-1))

        # Place an order
        order = market.place_order(c_id)
        self.assertEqual(order, [prod])
        self.assertEqual(market.prod_q, {"prod0": 0})
        self.assertEqual(market.carts, {0: []})

        # Attempt to place another order with an empty cart
        self.assertEqual(market.place_order(c_id), [])
        self.assertEqual(market.prod_q, {"prod0": 0})
        self.assertEqual(market.carts, {0: []})

        # Publish another product
        prod2 = "product2"
        market.publish(p_id_str, prod2)
        market.add_to_cart(c_id, prod2)

        # Place another order
        order = market.place_order(c_id)
        self.assertEqual(order, [prod2])
        self.assertEqual(market.carts, {0: []})


# Attention. You have to run manually each one
class TestFunctional(unittest.TestCase):
    # pylint: disable=too-many-instance-attributes
    # pylint: disable-msg=too-many-locals
    """ Functional testing """

    # DON'T USE, IT'S JUST A HELPER
    def test_functional(self, input_file, output_file, ref_file):
        """Test individual json file"""

        # Load test case input data
        with open(input_file, 'r', encoding='utf-8') as f:
            test_data = json.load(f)

        # Create marketplace instance
        market = Marketplace(test_data['marketplace']['queue_size_per_producer'])

        # Create a dictionary to map product IDs to their names
        products = {prod_id: prod_data['name'] for prod_id, prod_data in
                    test_data['products'].items()}

        # Register producers
        for producer_data in test_data['producers']:
            p_id = market.register_producer()
            for product_info in producer_data['products']:
                product_id, quantity, _ = product_info
                product_name = products[product_id]
                for _ in range(quantity):
                    market.publish(p_id, product_name)

        # Register consumers
        for consumer_data in test_data['consumers']:
            c_id = market.new_cart()
            for cart in consumer_data['carts']:
                for cart_item in cart['ops']:
                    product_id = cart_item['product']
                    product_name = products[product_id]
                    if cart_item['type'] == 'add':
                        for _ in range(cart_item['quantity']):
                            market.add_to_cart(c_id, product_name)
                    elif cart_item['type'] == 'remove':
                        for _ in range(cart_item['quantity']):
                            market.remove_from_cart(c_id, product_name)

                market.place_order(c_id)

        # Check test case output data
        with open(output_file, 'r', encoding='utf-8') as f:
            expected_output = f.read().strip().split('\n')

        # Check test case output data
        with open(ref_file, 'r', encoding='utf-8') as f:
            ref = f.read().strip().split('\n')

        self.assertEqual(ref, expected_output)

        time.sleep(0.5)

    def test_specific_test(self):
        """ Test a specific test """
        # Insert here
        file_nr = 1

        # Don't touch from here
        input_file = ""
        output_file = ""
        ref_file = ""

        if file_nr < 10:
            input_file = '../tests/0' + str(file_nr) + '.json'
            output_file = '../tests/0' + str(file_nr) + '.out.sorted'
            ref_file = '../tests/0' + str(file_nr) + '.ref.out'
        else:
            input_file = '../tests/' + str(file_nr) + '.json'
            output_file = '../tests/' + str(file_nr) + '.out.sorted'
            ref_file = '../tests/' + str(file_nr) + '.ref.out'

        self.test_functional(input_file, output_file, ref_file)
