"""
Consumer
"""

from threading import Thread
from time import sleep


class Consumer(Thread):
    """
    Consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Create consumer

        Args:
            carts (list): A list of dictionaries that contain the operations
            to be performed on each cart.
            marketplace (Marketplace): An instance of the Marketplace class.
            retry_wait_time (float): The amount of time to wait before retrying
            an operation.
            **kwargs: Keyword arguments that will be passed to the parent class
            (Thread).

        Returns:
            None.
        """
        super().__init__(**kwargs)
        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time

    def run(self):
        """
        Run Consumer.

        Returns:
            None.
        """
        for cart in self.carts:
            cart_id = self.marketplace.new_cart()

            for operation in cart:
                operation_type = operation["type"]
                product = operation["product"]
                quantity = operation["quantity"]

                if operation_type == "add":
                    for _ in range(quantity):
                        while not self.marketplace.add_to_cart(cart_id, product):
                            sleep(self.retry_wait_time)
                elif operation_type == "remove":
                    for _ in range(quantity):
                        self.marketplace.remove_from_cart(cart_id, product)

            order = self.marketplace.place_order(cart_id)
            for ordered_product in order:
                print(f"{self.name} bought {ordered_product}")
