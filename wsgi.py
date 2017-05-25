from linkapp.link.wsgi import LinkMicroservice
from linkapp.link.config import LinkConfig

config = LinkConfig()

app = LinkMicroservice(config)