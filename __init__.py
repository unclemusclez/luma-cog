from .core.luma import Luma


async def setup(bot):
    """
    Luma Cog setup for RedBot 3.5+

    :param bot: The RedBot instance
    """
    await bot.add_cog(Luma(bot))
