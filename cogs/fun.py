import discord
from discord.ext import commands

class Fun_Commands(commands.Cog, name='Fun Commands'):
    def __init__(self, bot):
        self.bot = bot
    @commands.command(help=f'''Hug a user! The bot will then tag the user for
    hugs''', brief='Hug a user!')
    async def hug(self, ctx, *, user: discord.Member = None):
        if user is not None:
            await ctx.send(f'Sending hugs to <@!{user.id}> <:people_hugging:807577128077885461>')
        else:
            await ctx.send('Hugs for who?')

    @hug.error
    async def hug_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send("I don't know who that is.")
        else:
            raise error

async def setup(bot):
    await bot.add_cog(Fun_Commands(bot))
