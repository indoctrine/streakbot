import discord
from discord.ext import commands
import time

class Utility_Commands(commands.Cog, name='Utility Commands'):
    def __init__(self, bot):
        self.bot = bot
        self.bot.help_command.cog = self
    @commands.command(help='Ping the bot to get the current latency',brief='Ping the bot')
    async def ping(self, ctx):
        #await ctx.send(f'Pong! Latency is {round(ctx.bot.latency*1000)}ms')
        t_1 = time.perf_counter()
        await ctx.author.trigger_typing()  # tell Discord that the bot is "typing", which is a very simple request
        t_2 = time.perf_counter()
        time_delta = round((t_2-t_1)*1000)  # calculate the time needed to trigger typing
        await ctx.send(f"Pong! Latency is {time_delta}ms")

def setup(bot):
    bot.add_cog(Utility_Commands(bot))
