import asyncio
from typing import Awaitable, Optional


class AsyncTaskHelper:
    async def define_and_gather_task(
        self, coroutine: Awaitable, args_list, args_to_ret_inds=None
    ):
        tasks = [
            self.safe_coro(coroutine, *_args, args_to_ret_inds=args_to_ret_inds)
            for _args in args_list
        ]

        return await asyncio.gather(*tasks)

    async def safe_coro(
        self,
        coroutine: Awaitable,
        *args,
        args_to_ret_inds: Optional[list[int]] = None,
        kwargs_to_ret_keys: Optional[list[str]] = None,
        **kwargs,
    ):
        try:
            res = await coroutine(*args, **kwargs)
            success = True
        except Exception:
            success = False
            res = None
        if args_to_ret_inds:
            args_to_ret = [args[i] for i in args_to_ret_inds]
        else:
            args_to_ret = []
        if kwargs_to_ret_keys:
            kwargs_to_ret = {k: kwargs[k] for k in kwargs_to_ret_keys}
        else:
            kwargs_to_ret = {}
        return success, res, args_to_ret, kwargs_to_ret
