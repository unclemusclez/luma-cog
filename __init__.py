from luma import Luma
from . import models, api_client

def setup(bot):
    bot.add_cog(Luma(bot))
